"""
URL configuration for backend project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from api.views import (ingest_metrics, list_servers, latest_metrics, historical_metrics,
    alert_rules, alert_rule_detail, open_alerts, alert_history, resolve_alert_view,
    ingest_logs, list_logs,)


urlpatterns = [
    path("admin/", admin.site.urls),

    # Metrics
    path("api/metrics/ingest/", ingest_metrics, name="metrics-ingest"),
    path("api/servers/", list_servers, name="list-servers"),
    path("api/metrics/latest/", latest_metrics, name="latest-metrics"),
    path("api/metrics/history/", historical_metrics, name="historical-metrics"),

    # Alerts
    path("api/alerts/rules/", alert_rules, name="alert-rules"),
    path("api/alerts/rules/<uuid:rule_id>/", alert_rule_detail, name="alert-rule-detail"),
    path("api/alerts/open/", open_alerts, name="open-alerts"),
    path("api/alerts/history/", alert_history, name="alert-history"),
    path("api/alerts/resolve/", resolve_alert_view, name="resolve-alert"),
    
    # Logs
    path("api/logs/ingest/", ingest_logs, name="logs-ingest"),
    path("api/logs/", list_logs, name="list-logs"),
]