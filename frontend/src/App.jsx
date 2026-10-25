import "./App.css";
import { BrandMark, Link } from "./components.jsx";
import { useLocation, useServers } from "./lib.js";
import OverviewPage from "./pages/OverviewPage.jsx";
import ServerPage from "./pages/ServerPage.jsx";
import AlertsPage from "./pages/AlertsPage.jsx";
import RulesPage from "./pages/RulesPage.jsx";
import LogsPage from "./pages/LogsPage.jsx";

const NAV = [
  { to: "/", label: "Overview" },
  { to: "/servers", label: "Servers" },
  { to: "/alerts", label: "Alerts" },
  { to: "/rules", label: "Alert rules" },
  { to: "/logs", label: "Logs" },
];

function isActive(path, to) {
  return to === "/" ? path === "/" : path === to || path.startsWith(`${to}/`);
}

function App() {
  const { path, query } = useLocation();
  const servers = useServers();

  let page;
  if (path === "/") {
    page = <OverviewPage servers={servers} />;
  } else if (path === "/servers" || path.startsWith("/servers/")) {
    const serverId = decodeURIComponent(path.slice("/servers/".length));
    page = <ServerPage servers={servers} serverId={serverId} />;
  } else if (path === "/alerts") {
    page = <AlertsPage servers={servers} />;
  } else if (path === "/rules") {
    page = <RulesPage servers={servers} />;
  } else if (path === "/logs") {
    page = <LogsPage servers={servers} query={query} />;
  } else {
    page = (
      <div className="panel section">
        <p className="empty">
          Page not found. <Link to="/">Go to the overview</Link>.
        </p>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="topbar">
        <div className="wrap topbar__inner">
          <Link to="/" className="brand">
            <BrandMark />
            <div>
              <div className="brand__name">CloudPulse</div>
              <div className="brand__tag">Live server monitoring on Apache Cassandra</div>
            </div>
          </Link>
          <nav className="nav" aria-label="Main">
            {NAV.map((item) => (
              <Link
                key={item.to}
                to={item.to}
                className={`nav__link ${isActive(path, item.to) ? "nav__link--active" : ""}`}
                aria-current={isActive(path, item.to) ? "page" : undefined}
              >
                {item.label}
              </Link>
            ))}
          </nav>
        </div>
      </header>

      <main className="wrap">
        {servers.error && (
          <div className="banner" role="alert">
            <strong>Can't reach the CloudPulse API</strong>
            {servers.error}. Check that Django is running on port 8000 and the
            Cassandra cluster is up.
          </div>
        )}
        {page}
      </main>

      <footer className="footer">
        <div className="wrap">
          CloudPulse, built with React, Django and a 3-node Apache Cassandra cluster.
        </div>
      </footer>
    </div>
  );
}

export default App;