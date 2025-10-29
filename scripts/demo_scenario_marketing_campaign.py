#!/usr/bin/env python3
"""
Arc Demo Scenario: Marketing Campaign Impact

This script generates a realistic observability scenario showing how a marketing campaign
affects your system across all four data types: metrics, logs, traces, and events.

Scenario Timeline:
- T-30min: Normal operations (baseline)
- T=0: Marketing campaign starts (event)
- T+0 to T+30min: Increased load, some errors, higher latency
- T+30min: Campaign ends, system stabilizes

This demonstrates Arc's unified observability by correlating:
- Metrics: CPU, memory, request rate
- Logs: Application errors and warnings
- Traces: Request latency across services
- Events: Campaign start/end, deployments, alerts
"""

import msgpack
import aiohttp
import asyncio
import argparse
import gzip
import random
import time
from datetime import datetime, timedelta
import uuid


class MarketingCampaignScenario:
    """Generates realistic observability data for a marketing campaign scenario"""

    def __init__(self, base_time: int, arc_url: str, token: str, database_prefix: str):
        """
        Args:
            base_time: Base timestamp in milliseconds (campaign start time)
            arc_url: Arc server URL
            token: API token
            database_prefix: Database name prefix (e.g., 'demo' creates 'demo_metrics', 'demo_logs', etc.)
        """
        self.base_time = base_time
        self.arc_url = arc_url
        self.token = token
        self.database_prefix = database_prefix
        self.session = None

        # Separate databases for each data type
        self.databases = {
            "metrics": f"{database_prefix}_metrics",
            "logs": f"{database_prefix}_logs",
            "traces": f"{database_prefix}_traces",
            "events": f"{database_prefix}_events"
        }

        # Scenario parameters
        self.campaign_duration_ms = 30 * 60 * 1000  # 30 minutes
        self.baseline_duration_ms = 30 * 60 * 1000  # 30 minutes before

        # Services in our system
        self.services = [
            "api-gateway",
            "order-service",
            "payment-service",
            "inventory-service",
            "user-service",
            "database"
        ]

        # Hosts
        self.hosts = [f"prod-{i:02d}" for i in range(1, 6)]  # 5 hosts

    async def setup(self):
        """Setup HTTP session"""
        self.session = aiohttp.ClientSession()

    async def cleanup(self):
        """Cleanup HTTP session"""
        if self.session:
            await self.session.close()

    async def send_data(self, measurement: str, payload: dict, database: str):
        """Send data to Arc

        Args:
            measurement: Measurement name (for logging)
            payload: MessagePack payload
            database: Target database name
        """
        # Serialize to MessagePack
        data = msgpack.packb(payload)

        # Compress with gzip
        compressed = gzip.compress(data)

        headers = {
            "Content-Type": "application/msgpack",
            "Content-Encoding": "gzip",
            "X-Arc-Database": database,
            "Authorization": f"Bearer {self.token}"
        }

        async with self.session.post(
            f"{self.arc_url}/api/v1/write/msgpack",
            data=compressed,
            headers=headers
        ) as response:
            if response.status not in (200, 204):
                text = await response.text()
                print(f"❌ Error sending {measurement} to {database}: {response.status} - {text}")
            return response.status in (200, 204)

    def generate_events(self):
        """Generate campaign events"""
        events = []

        # Campaign start event
        events.append({
            "time": self.base_time,
            "event_type": "marketing_campaign_started",
            "event_category": "business",
            "severity": "info",
            "source": "marketing-automation",
            "environment": "production",
            "user_id": "",
            "resource_id": "campaign-black-friday-2024",
            "metadata": '{"campaign_name":"Black Friday 2024","target_audience":"all_users","expected_traffic":"4x normal","channels":["email","push","social"]}',
            "duration_ms": self.campaign_duration_ms,
            "success": True,
            "amount": 0.0
        })

        # Auto-scaling event (system responds to load)
        events.append({
            "time": self.base_time + 5 * 60 * 1000,  # 5 min after start
            "event_type": "autoscale_triggered",
            "event_category": "infrastructure",
            "severity": "info",
            "source": "kubernetes",
            "environment": "production",
            "user_id": "",
            "resource_id": "order-service",
            "metadata": '{"from_replicas":3,"to_replicas":8,"reason":"CPU > 80%"}',
            "duration_ms": 120000,  # 2 minutes to scale
            "success": True,
            "amount": 0.0
        })

        # Alert event (connection pool exhaustion)
        events.append({
            "time": self.base_time + 10 * 60 * 1000,  # 10 min after start
            "event_type": "alert_triggered",
            "event_category": "infrastructure",
            "severity": "warning",
            "source": "prometheus",
            "environment": "production",
            "user_id": "",
            "resource_id": "database",
            "metadata": '{"alert":"DatabaseConnectionPoolExhausted","threshold":90,"current":95,"severity":"warning"}',
            "duration_ms": 15 * 60 * 1000,  # Alert active for 15 min
            "success": False,
            "amount": 0.0
        })

        # Campaign end event
        events.append({
            "time": self.base_time + self.campaign_duration_ms,
            "event_type": "marketing_campaign_ended",
            "event_category": "business",
            "severity": "info",
            "source": "marketing-automation",
            "environment": "production",
            "user_id": "",
            "resource_id": "campaign-black-friday-2024",
            "metadata": '{"total_orders":48320,"revenue":1247500,"conversion_rate":"12.5%"}',
            "duration_ms": 0,
            "success": True,
            "amount": 1247500.0
        })

        return events

    def generate_metrics(self, num_points: int = 180):
        """Generate CPU and memory metrics showing campaign impact

        Args:
            num_points: Number of data points (default: 180 = 1 hour at 20s intervals)
        """
        metrics = []
        interval_ms = 20000  # 20 seconds
        start_time = self.base_time - self.baseline_duration_ms

        for i in range(num_points):
            current_time = start_time + (i * interval_ms)

            # Determine campaign phase
            time_into_campaign = current_time - self.base_time

            for host in self.hosts:
                # CPU usage
                if time_into_campaign < 0:
                    # Baseline: 20-30% CPU
                    cpu_value = random.uniform(20, 30)
                elif time_into_campaign < self.campaign_duration_ms:
                    # During campaign: 75-95% CPU
                    cpu_value = random.uniform(75, 95)
                else:
                    # After campaign: returning to normal
                    cpu_value = random.uniform(25, 40)

                metrics.append({
                    "time": current_time,
                    "host": host,
                    "metric": "cpu_usage",
                    "value": cpu_value,
                    "environment": "production"
                })

                # Memory usage
                if time_into_campaign < 0:
                    mem_value = random.uniform(40, 50)
                elif time_into_campaign < self.campaign_duration_ms:
                    mem_value = random.uniform(65, 85)
                else:
                    mem_value = random.uniform(45, 55)

                metrics.append({
                    "time": current_time,
                    "host": host,
                    "metric": "memory_usage",
                    "value": mem_value,
                    "environment": "production"
                })

        return metrics

    def generate_logs(self, num_logs: int = 5000):
        """Generate application logs showing errors during campaign

        Args:
            num_logs: Number of log entries to generate
        """
        logs = []
        start_time = self.base_time - self.baseline_duration_ms

        log_templates = {
            "info": [
                "Request completed successfully",
                "Order processed",
                "Payment authorized",
                "User authenticated",
                "Cache hit"
            ],
            "warning": [
                "High connection pool usage: {usage}%",
                "Slow query detected: {duration}ms",
                "Cache miss rate increasing",
                "Rate limit approaching for IP {ip}"
            ],
            "error": [
                "Connection timeout to database",
                "Payment gateway timeout",
                "Connection pool exhausted",
                "Failed to acquire database connection",
                "Order processing failed: timeout"
            ]
        }

        for i in range(num_logs):
            # Distribute logs across the timeline
            current_time = start_time + int((self.baseline_duration_ms + self.campaign_duration_ms + self.baseline_duration_ms) * (i / num_logs))
            time_into_campaign = current_time - self.base_time

            # More errors during campaign
            if time_into_campaign < 0:
                # Baseline: mostly info, few warnings
                level_choice = random.choices(
                    ["info", "warning", "error"],
                    weights=[0.90, 0.09, 0.01]
                )[0]
            elif time_into_campaign < self.campaign_duration_ms:
                # During campaign: more warnings and errors
                level_choice = random.choices(
                    ["info", "warning", "error"],
                    weights=[0.70, 0.20, 0.10]
                )[0]
            else:
                # After campaign: returning to normal
                level_choice = random.choices(
                    ["info", "warning", "error"],
                    weights=[0.85, 0.12, 0.03]
                )[0]

            # Select message template
            message = random.choice(log_templates[level_choice])
            if "{usage}" in message:
                message = message.format(usage=random.randint(85, 98))
            if "{duration}" in message:
                message = message.format(duration=random.randint(1000, 5000))
            if "{ip}" in message:
                message = message.format(ip=f"192.168.1.{random.randint(1, 254)}")

            logs.append({
                "time": current_time,
                "level": level_choice.upper(),
                "message": message,
                "service": random.choice(self.services),
                "host": random.choice(self.hosts),
                "environment": "production"
            })

        return logs

    def generate_traces(self, num_traces: int = 1000):
        """Generate distributed traces showing latency impact

        Args:
            num_traces: Number of traces to generate
        """
        spans = []
        start_time = self.base_time - self.baseline_duration_ms

        for i in range(num_traces):
            # Distribute traces across timeline
            current_time = start_time + int((self.baseline_duration_ms + self.campaign_duration_ms + self.baseline_duration_ms) * (i / num_traces))
            time_into_campaign = current_time - self.base_time

            trace_id = str(uuid.uuid4())

            # Determine latency multiplier based on campaign phase
            if time_into_campaign < 0:
                latency_multiplier = 1.0  # Normal
            elif time_into_campaign < self.campaign_duration_ms:
                latency_multiplier = random.uniform(3.0, 8.0)  # 3-8x slower
            else:
                latency_multiplier = random.uniform(1.0, 1.5)  # Slightly elevated

            # Generate spans for this trace
            # API Gateway (root span)
            gateway_duration_ns = int(random.uniform(10, 50) * 1_000_000 * latency_multiplier)
            spans.append({
                "time": current_time,
                "trace_id": trace_id,
                "span_id": f"span-{uuid.uuid4().hex[:8]}",
                "parent_span_id": "",
                "service_name": "api-gateway",
                "operation_name": "POST /orders",
                "span_kind": "server",
                "duration_ns": gateway_duration_ns,
                "status_code": 200 if random.random() > 0.05 else 500,
                "http_method": "POST",
                "environment": "production",
                "region": "us-east-1",
                "error": random.random() < 0.05 if time_into_campaign >= 0 and time_into_campaign < self.campaign_duration_ms else False
            })

            # Order service
            gateway_span_id = spans[-1]["span_id"]
            order_duration_ns = int(random.uniform(30, 100) * 1_000_000 * latency_multiplier)
            spans.append({
                "time": current_time + 1,
                "trace_id": trace_id,
                "span_id": f"span-{uuid.uuid4().hex[:8]}",
                "parent_span_id": gateway_span_id,
                "service_name": "order-service",
                "operation_name": "create_order",
                "span_kind": "internal",
                "duration_ns": order_duration_ns,
                "status_code": 200,
                "http_method": "",
                "environment": "production",
                "region": "us-east-1",
                "error": False
            })

            # Database query (slow during campaign)
            order_span_id = spans[-1]["span_id"]
            db_duration_ns = int(random.uniform(5, 20) * 1_000_000 * latency_multiplier)
            spans.append({
                "time": current_time + 5,
                "trace_id": trace_id,
                "span_id": f"span-{uuid.uuid4().hex[:8]}",
                "parent_span_id": order_span_id,
                "service_name": "database",
                "operation_name": "INSERT",
                "span_kind": "client",
                "duration_ns": db_duration_ns,
                "status_code": 0,
                "http_method": "",
                "environment": "production",
                "region": "us-east-1",
                "error": False
            })

        return spans

    async def run_scenario(self):
        """Execute the complete scenario"""
        print("🎬 Arc Demo Scenario: Marketing Campaign Impact")
        print("=" * 60)
        print(f"📅 Campaign start time: {datetime.fromtimestamp(self.base_time / 1000)}")
        print(f"⏱️  Timeline: 30 min before → 30 min during → 30 min after")
        print(f"🎯 Target: {self.arc_url}")
        print(f"💾 Databases:")
        print(f"   - Events: {self.databases['events']}")
        print(f"   - Metrics: {self.databases['metrics']}")
        print(f"   - Logs: {self.databases['logs']}")
        print(f"   - Traces: {self.databases['traces']}")
        print()

        # Generate all data
        print("🔄 Generating scenario data...")
        events = self.generate_events()
        metrics = self.generate_metrics(num_points=180)  # 1 hour of metrics
        logs = self.generate_logs(num_logs=5000)
        spans = self.generate_traces(num_traces=1000)

        print(f"  ✅ Events: {len(events)}")
        print(f"  ✅ Metrics: {len(metrics)}")
        print(f"  ✅ Logs: {len(logs)}")
        print(f"  ✅ Traces: {len(spans)} spans")
        print()

        # Send events
        print("📤 Sending events...")
        events_payload = {
            "m": "system_events",
            "columns": {
                "time": [e["time"] for e in events],
                "event_type": [e["event_type"] for e in events],
                "event_category": [e["event_category"] for e in events],
                "severity": [e["severity"] for e in events],
                "source": [e["source"] for e in events],
                "environment": [e["environment"] for e in events],
                "user_id": [e["user_id"] for e in events],
                "resource_id": [e["resource_id"] for e in events],
                "metadata": [e["metadata"] for e in events],
                "duration_ms": [e["duration_ms"] for e in events],
                "success": [e["success"] for e in events],
                "amount": [e["amount"] for e in events]
            }
        }
        success = await self.send_data("system_events", events_payload, self.databases['events'])
        print(f"  {'✅' if success else '❌'} Events sent to {self.databases['events']}")

        # Send metrics in batches
        print("📤 Sending metrics...")
        batch_size = 1000
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i + batch_size]
            metrics_payload = {
                "m": "system_metrics",
                "columns": {
                    "time": [m["time"] for m in batch],
                    "host": [m["host"] for m in batch],
                    "metric": [m["metric"] for m in batch],
                    "value": [m["value"] for m in batch],
                    "environment": [m["environment"] for m in batch]
                }
            }
            await self.send_data("system_metrics", metrics_payload, self.databases['metrics'])
        print(f"  ✅ Metrics sent to {self.databases['metrics']} ({len(metrics)} points)")

        # Send logs in batches
        print("📤 Sending logs...")
        for i in range(0, len(logs), batch_size):
            batch = logs[i:i + batch_size]
            logs_payload = {
                "m": "application_logs",
                "columns": {
                    "time": [l["time"] for l in batch],
                    "level": [l["level"] for l in batch],
                    "message": [l["message"] for l in batch],
                    "service": [l["service"] for l in batch],
                    "host": [l["host"] for l in batch],
                    "environment": [l["environment"] for l in batch]
                }
            }
            await self.send_data("application_logs", logs_payload, self.databases['logs'])
        print(f"  ✅ Logs sent to {self.databases['logs']} ({len(logs)} entries)")

        # Send traces in batches
        print("📤 Sending traces...")
        for i in range(0, len(spans), batch_size):
            batch = spans[i:i + batch_size]
            traces_payload = {
                "m": "distributed_traces",
                "columns": {
                    "time": [s["time"] for s in batch],
                    "trace_id": [s["trace_id"] for s in batch],
                    "span_id": [s["span_id"] for s in batch],
                    "parent_span_id": [s["parent_span_id"] for s in batch],
                    "service_name": [s["service_name"] for s in batch],
                    "operation_name": [s["operation_name"] for s in batch],
                    "span_kind": [s["span_kind"] for s in batch],
                    "duration_ns": [s["duration_ns"] for s in batch],
                    "status_code": [s["status_code"] for s in batch],
                    "http_method": [s["http_method"] for s in batch],
                    "environment": [s["environment"] for s in batch],
                    "region": [s["region"] for s in batch],
                    "error": [s["error"] for s in batch]
                }
            }
            await self.send_data("distributed_traces", traces_payload, self.databases['traces'])
        print(f"  ✅ Traces sent to {self.databases['traces']} ({len(spans)} spans)")

        print()
        print("✨ Scenario complete!")
        print()
        print("🔍 Try these queries to explore the data:")
        print()
        print("1. Find events during CPU spikes (cross-database join):")
        print(f"   SELECT e.time, e.event_type, m.value")
        print(f"   FROM {self.databases['events']}.system_events e")
        print(f"   JOIN {self.databases['metrics']}.system_metrics m")
        print("     ON m.time BETWEEN e.time AND e.time + 300000")
        print("   WHERE m.metric = 'cpu_usage' AND m.value > 80;")
        print()
        print("2. Show errors during campaign (cross-database join):")
        print(f"   SELECT e.time as campaign_start, l.time as log_time, l.level, l.message")
        print(f"   FROM {self.databases['events']}.system_events e")
        print(f"   JOIN {self.databases['logs']}.application_logs l")
        print("     ON l.time BETWEEN e.time AND e.time + e.duration_ms")
        print("   WHERE e.event_type = 'marketing_campaign_started' AND l.level = 'ERROR';")
        print()
        print("3. Compare latency before/during/after (cross-database join):")
        print("   SELECT")
        print("     CASE")
        print("       WHEN t.time < e.time THEN 'before_campaign'")
        print("       WHEN t.time BETWEEN e.time AND e.time + 1800000 THEN 'during_campaign'")
        print("       ELSE 'after_campaign'")
        print("     END as phase,")
        print("     AVG(t.duration_ns / 1000000.0) as avg_latency_ms")
        print(f"   FROM {self.databases['traces']}.distributed_traces t")
        print(f"   CROSS JOIN {self.databases['events']}.system_events e")
        print("   WHERE e.event_type = 'marketing_campaign_started'")
        print("     AND t.service_name = 'api-gateway'")
        print("   GROUP BY phase;")



async def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic marketing campaign scenario for Arc demo"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Arc server URL (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--token",
        required=True,
        help="Arc API token"
    )
    parser.add_argument(
        "--database",
        default="demo",
        help="Database name prefix (default: demo, creates demo_metrics, demo_logs, demo_traces, demo_events)"
    )
    parser.add_argument(
        "--campaign-time",
        type=int,
        help="Campaign start time in Unix milliseconds (default: current time)"
    )

    args = parser.parse_args()

    # Use provided time or current time
    campaign_time = args.campaign_time if args.campaign_time else int(time.time() * 1000)

    scenario = MarketingCampaignScenario(
        base_time=campaign_time,
        arc_url=args.url,
        token=args.token,
        database_prefix=args.database
    )

    await scenario.setup()
    try:
        await scenario.run_scenario()
    finally:
        await scenario.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
