import time
from datetime import datetime, timezone

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Build 1-minute and 1-hour metric rollups from raw metrics."

    def add_arguments(self, parser):
        parser.add_argument(
            "--org", action="append", dest="orgs",
            help="Organization ID (repeatable). Default: local",
        )
        parser.add_argument(
            "--loop", action="store_true",
            help="Keep running: one rollup pass every minute",
        )
        parser.add_argument(
            "--backfill-hours", type=int, default=0,
            help="First rebuild this many past hours (e.g. 24)",
        )

    def handle(self, *args, **options):
        from api.rollups import run_rollups   # imported after Django setup

        orgs = options["orgs"] or ["local"]

        if options["backfill_hours"] > 0:
            summary = run_rollups(
                orgs, datetime.now(timezone.utc),
                minutes_back=options["backfill_hours"] * 60,
            )
            self.stdout.write(f"Backfill done: {summary}")
        elif not options["loop"]:
            summary = run_rollups(orgs, datetime.now(timezone.utc))
            self.stdout.write(f"Rollup done: {summary}")

        if not options["loop"]:
            return

        self.stdout.write("Rollup loop started (runs at :05 past every minute). Ctrl+C to stop.")
        while True:
            # Wait until 5 s past the next minute so that minute is complete.
            now = datetime.now(timezone.utc)
            delay = (5 - now.second - now.microsecond / 1e6) % 60
            time.sleep(delay if delay >= 1 else delay + 60)

            try:
                summary = run_rollups(orgs, datetime.now(timezone.utc))
                self.stdout.write(
                    f"{datetime.now():%H:%M:%S} | servers {summary['servers']} "
                    f"| 1m rows {summary['minute_rows']} "
                    f"| 1h rows {summary['hour_rows']} "
                    f"| window {summary['window']} | {summary['ms']} ms"
                )
            except Exception as exc:
                self.stderr.write(f"Rollup failed: {exc}")