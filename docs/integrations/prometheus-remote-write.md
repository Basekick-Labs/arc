# Prometheus Remote Write

The documentation for Arc's Prometheus Remote Write 1.0 receiver
(`POST /api/v1/write/prometheus`, Arc 27.01.1 and later) is on the docs site:

- Arc: https://docs.basekick.net/arc/integrations/prometheus-remote-write/
- Arc Enterprise clusters: https://docs.basekick.net/arc-enterprise/integrations/prometheus-remote-write/

Both pages cover a quick start with Prometheus and Grafana Alloy, the required
headers, how series map to tables and columns, what is not stored and why, the
request limits, monitoring and troubleshooting. The Enterprise page adds
RBAC-scoped tokens, forwarding from readers and standby writers, and writer
failover.
