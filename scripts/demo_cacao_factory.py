#!/usr/bin/env python3
"""
Arc Demo: F. Pache Cacao Factory - Complete Production Monitoring

This script simulates a comprehensive observability scenario for a cacao processing factory,
modeling the entire production chain from raw cacao beans to finished chocolate products.

F. Pache is a Uruguayan cacao processor and chocolate manufacturer.

Production Chain (Based on F. Pache operations):
1. Raw Material Reception (Cacao beans from different origins)
2. Cleaning & Sorting
3. Roasting
4. Grinding & Conching
5. Tempering
6. Molding & Packaging
7. Quality Control
8. Warehouse & Distribution

Data Types Generated:
- Metrics: Production rates, machine utilization, quality metrics, inventory levels
- Logs: Equipment status, quality issues, operator actions
- Traces: Product batch tracing through production stages
- Events: Shift changes, equipment maintenance, quality alerts, sales orders

Cacao Types (from F. Pache catalog):
- Cacao del Plata (Río de la Plata region)
- Cacao Misiones (Premium origin)
- Cacao Blend (Mixed origins)

Products:
- Chocolate Bars (70%, 85%, 100%)
- Chocolate Chips
- Cocoa Powder
- Cocoa Butter
- Nibs

Business Metrics:
- Production against daily targets
- Stock levels vs. sales orders
- Quality compliance rates
- OEE (Overall Equipment Effectiveness)
- Predictive stock alerts
"""

import msgpack
import aiohttp
import asyncio
import argparse
import gzip
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid
import math


class CacaoFactoryScenario:
    """Generates realistic production monitoring data for a cacao factory"""

    def __init__(self, arc_url: str, token: str, database_prefix: str = "fpache"):
        self.arc_url = arc_url
        self.token = token
        self.database_prefix = database_prefix
        self.session = None

        # Databases for each data type
        self.databases = {
            "metrics": f"{database_prefix}_metrics",
            "logs": f"{database_prefix}_logs",
            "traces": f"{database_prefix}_traces",
            "events": f"{database_prefix}_events"
        }

        # Production lines
        self.production_lines = {
            "line_1": {"name": "Chocolate Bars Line", "capacity_kg_h": 500},
            "line_2": {"name": "Powder & Chips Line", "capacity_kg_h": 300},
            "line_3": {"name": "Premium Products Line", "capacity_kg_h": 200}
        }

        # Cacao origins (from F. Pache)
        self.cacao_origins = [
            "cacao_del_plata",    # Argentine origin
            "cacao_misiones",      # Premium Misiones
            "cacao_salta",         # Salta region
            "cacao_blend"          # Mixed origins
        ]

        # Products catalog
        self.products = {
            "bar_70": {"name": "Chocolate Bar 70%", "cacao_kg": 0.07, "price_usd": 4.5, "line": "line_1"},
            "bar_85": {"name": "Chocolate Bar 85%", "cacao_kg": 0.085, "price_usd": 5.2, "line": "line_1"},
            "bar_100": {"name": "Chocolate Bar 100%", "cacao_kg": 0.1, "price_usd": 6.0, "line": "line_1"},
            "chips_dark": {"name": "Dark Chocolate Chips", "cacao_kg": 0.08, "price_usd": 8.5, "line": "line_2"},
            "cocoa_powder": {"name": "Cocoa Powder 1kg", "cacao_kg": 1.0, "price_usd": 12.0, "line": "line_2"},
            "cocoa_butter": {"name": "Cocoa Butter 500g", "cacao_kg": 0.5, "price_usd": 15.0, "line": "line_3"},
            "nibs": {"name": "Cacao Nibs 250g", "cacao_kg": 0.25, "price_usd": 7.0, "line": "line_3"}
        }

        # Equipment/machines
        self.equipment = {
            "roaster_1": {"type": "roaster", "capacity_kg_h": 200, "line": "line_1"},
            "roaster_2": {"type": "roaster", "capacity_kg_h": 150, "line": "line_2"},
            "grinder_1": {"type": "grinder", "capacity_kg_h": 180, "line": "line_1"},
            "grinder_2": {"type": "grinder", "capacity_kg_h": 120, "line": "line_2"},
            "conche_1": {"type": "conche", "capacity_kg_h": 150, "line": "line_1"},
            "tempering_1": {"type": "tempering", "capacity_kg_h": 160, "line": "line_1"},
            "packaging_1": {"type": "packaging", "capacity_units_h": 2000, "line": "line_1"},
            "packaging_2": {"type": "packaging", "capacity_units_h": 1500, "line": "line_2"}
        }

        # Shifts
        self.shifts = {
            "morning": {"start_hour": 6, "end_hour": 14, "operators": 15},
            "afternoon": {"start_hour": 14, "end_hour": 22, "operators": 12},
            "night": {"start_hour": 22, "end_hour": 6, "operators": 8}
        }

        # Initial stock (in kg for raw materials, units for finished products)
        self.stock = {
            # Raw materials (kg)
            "cacao_del_plata": 5000,
            "cacao_misiones": 3000,
            "cacao_salta": 2500,
            "cacao_blend": 4000,
            "sugar": 8000,
            "cocoa_butter_raw": 2000,

            # Finished products (units)
            "bar_70": 2500,
            "bar_85": 1800,
            "bar_100": 1200,
            "chips_dark": 1500,
            "cocoa_powder": 800,
            "cocoa_butter": 600,
            "nibs": 1000
        }

        # Sales orders (simulated daily orders)
        self.pending_orders = []

        # Daily production targets (units)
        self.daily_targets = {
            "bar_70": 1500,
            "bar_85": 1000,
            "bar_100": 800,
            "chips_dark": 1200,
            "cocoa_powder": 500,
            "cocoa_butter": 400,
            "nibs": 600
        }

        # Current production counters
        self.production_today = {product: 0 for product in self.products.keys()}

    async def setup(self):
        """Setup HTTP session"""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=100)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def cleanup(self):
        """Cleanup HTTP session"""
        if self.session:
            await self.session.close()

    async def send_data(self, payload: dict, database: str):
        """Send data to Arc using MessagePack columnar format"""
        data = msgpack.packb(payload)
        compressed = gzip.compress(data)

        headers = {
            "Content-Type": "application/msgpack",
            "Content-Encoding": "gzip",
            "X-Arc-Database": database,
            "x-api-key": self.token
        }

        try:
            async with self.session.post(
                f"{self.arc_url}/api/v1/write/msgpack",
                data=compressed,
                headers=headers
            ) as resp:
                if resp.status != 204:
                    error_body = await resp.text()
                    print(f"❌ Error sending data: {resp.status} - {error_body}")
        except Exception as e:
            print(f"❌ Exception sending data: {e}")

    def get_current_shift(self, timestamp_ms: int) -> str:
        """Determine current shift based on timestamp"""
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        hour = dt.hour

        if 6 <= hour < 14:
            return "morning"
        elif 14 <= hour < 22:
            return "afternoon"
        else:
            return "night"

    async def generate_production_metrics(self, start_time_ms: int, duration_minutes: int, batch_size: int = 100):
        """Generate production metrics for all equipment and lines"""
        print(f"\n📊 Generating production metrics...")

        total_points = duration_minutes * 60 // 5  # One point every 5 seconds
        batches = (total_points + batch_size - 1) // batch_size

        for batch_idx in range(batches):
            times = []
            equipment_ids = []
            line_ids = []
            metric_names = []
            values = []
            units = []
            statuses = []

            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, total_points)

            for point_idx in range(batch_start, batch_end):
                timestamp_ms = start_time_ms + (point_idx * 5000)  # Every 5 seconds
                shift = self.get_current_shift(timestamp_ms)
                shift_info = self.shifts[shift]

                # Base efficiency by shift (morning is most efficient)
                shift_efficiency = {
                    "morning": 0.92,
                    "afternoon": 0.88,
                    "night": 0.82
                }[shift]

                # Generate metrics for each equipment
                for equipment_id, equipment_info in self.equipment.items():
                    eq_type = equipment_info["type"]
                    line = equipment_info["line"]

                    # Random variations
                    efficiency = shift_efficiency + random.uniform(-0.08, 0.05)
                    efficiency = max(0.5, min(0.98, efficiency))

                    # Temperature (for roasters)
                    if eq_type == "roaster":
                        temp = 120 + random.uniform(-5, 5)
                        times.append(timestamp_ms)
                        equipment_ids.append(equipment_id)
                        line_ids.append(line)
                        metric_names.append("temperature")
                        values.append(temp)
                        units.append("celsius")
                        statuses.append("running" if efficiency > 0.6 else "degraded")

                    # Throughput (kg/h or units/h)
                    if "capacity_kg_h" in equipment_info:
                        throughput = equipment_info["capacity_kg_h"] * efficiency
                    else:
                        throughput = equipment_info["capacity_units_h"] * efficiency

                    times.append(timestamp_ms)
                    equipment_ids.append(equipment_id)
                    line_ids.append(line)
                    metric_names.append("throughput")
                    values.append(round(throughput, 2))
                    units.append("kg_per_hour" if "capacity_kg_h" in equipment_info else "units_per_hour")
                    statuses.append("running" if efficiency > 0.6 else "degraded")

                    # Efficiency
                    times.append(timestamp_ms)
                    equipment_ids.append(equipment_id)
                    line_ids.append(line)
                    metric_names.append("efficiency")
                    values.append(round(efficiency * 100, 2))
                    units.append("percent")
                    statuses.append("running" if efficiency > 0.6 else "degraded")

                    # Power consumption
                    power_kw = {
                        "roaster": 45,
                        "grinder": 30,
                        "conche": 25,
                        "tempering": 15,
                        "packaging": 10
                    }[eq_type]

                    power = power_kw * efficiency + random.uniform(-2, 2)
                    times.append(timestamp_ms)
                    equipment_ids.append(equipment_id)
                    line_ids.append(line)
                    metric_names.append("power_consumption")
                    values.append(round(power, 2))
                    units.append("kw")
                    statuses.append("running" if efficiency > 0.6 else "degraded")

            # Send batch
            payload = {
                "m": "production_metrics",
                "columns": {
                    "time": times,
                    "equipment_id": equipment_ids,
                    "line_id": line_ids,
                    "metric_name": metric_names,
                    "value": values,
                    "unit": units,
                    "status": statuses
                }
            }

            await self.send_data(payload, self.databases["metrics"])

            if (batch_idx + 1) % 10 == 0:
                print(f"  📊 Sent {(batch_idx + 1) * batch_size} production metric points...")

        print(f"✅ Production metrics complete: {total_points:,} points")

    async def generate_quality_metrics(self, start_time_ms: int, duration_minutes: int, batch_size: int = 50):
        """Generate quality control metrics"""
        print(f"\n🔬 Generating quality metrics...")

        # Quality checks happen every 30 minutes
        total_checks = duration_minutes // 30
        batches = (total_checks + batch_size - 1) // batch_size

        for batch_idx in range(batches):
            times = []
            batch_ids = []
            products = []
            test_names = []
            values = []
            results = []
            inspectors = []

            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, total_checks)

            for check_idx in range(batch_start, batch_end):
                timestamp_ms = start_time_ms + (check_idx * 30 * 60 * 1000)
                batch_id = f"BATCH-{int(timestamp_ms/1000)}-{random.randint(100,999)}"

                # Random product
                product = random.choice(list(self.products.keys()))

                # Quality tests
                tests = {
                    "cocoa_percentage": (70, 100, random.uniform(68, 102)),
                    "moisture_content": (0, 5, random.uniform(1.5, 4.5)),
                    "particle_size": (15, 25, random.uniform(14, 26)),
                    "viscosity": (2000, 4000, random.uniform(1900, 4100)),
                    "melting_point": (31, 36, random.uniform(30, 37))
                }

                inspector = random.choice(["QC-Maria", "QC-Carlos", "QC-Ana", "QC-Luis"])

                for test_name, (min_val, max_val, measured) in tests.items():
                    times.append(timestamp_ms)
                    batch_ids.append(batch_id)
                    products.append(product)
                    test_names.append(test_name)
                    values.append(round(measured, 2))
                    results.append("pass" if min_val <= measured <= max_val else "fail")
                    inspectors.append(inspector)

            payload = {
                "m": "quality_metrics",
                "columns": {
                    "time": times,
                    "batch_id": batch_ids,
                    "product": products,
                    "test_name": test_names,
                    "value": values,
                    "result": results,
                    "inspector": inspectors
                }
            }

            await self.send_data(payload, self.databases["metrics"])

        print(f"✅ Quality metrics complete: {total_checks} quality checks")

    async def generate_stock_metrics(self, start_time_ms: int, duration_minutes: int):
        """Generate stock level metrics with consumption and replenishment"""
        print(f"\n📦 Generating stock metrics...")

        # Stock updates every 5 minutes
        total_updates = duration_minutes // 5
        current_stock = self.stock.copy()

        for update_idx in range(total_updates):
            timestamp_ms = start_time_ms + (update_idx * 5 * 60 * 1000)

            times = []
            items = []
            quantities = []
            types = []
            locations = []
            movements = []

            # Update stock based on production and sales
            for item, quantity in current_stock.items():
                # Raw materials consumption with periodic replenishment
                if item.startswith("cacao_") or item in ["sugar", "cocoa_butter_raw"]:
                    consumption = random.uniform(10, 50)  # kg consumed
                    # Replenish stock every ~4 hours (48 updates) to prevent depletion
                    replenishment = 0
                    if update_idx % 48 == 0 and update_idx > 0:
                        replenishment = random.uniform(500, 1500)  # Delivery arrives

                    current_stock[item] = max(0, quantity - consumption + replenishment)
                    movement = -consumption + replenishment
                else:
                    # Finished products: production - sales
                    production = random.uniform(5, 30)
                    sales = random.uniform(0, 20)
                    current_stock[item] = max(0, quantity + production - sales)
                    movement = production - sales

                times.append(timestamp_ms)
                items.append(item)
                quantities.append(round(current_stock[item], 2))
                types.append("raw_material" if item.startswith("cacao_") or item in ["sugar", "cocoa_butter_raw"] else "finished_product")
                locations.append("warehouse_A" if "raw" in item or item.startswith("cacao_") else "warehouse_B")
                movements.append(round(movement, 2))

            payload = {
                "m": "stock_levels",
                "columns": {
                    "time": times,
                    "item": items,
                    "quantity": quantities,
                    "type": types,
                    "location": locations,
                    "movement": movements
                }
            }

            await self.send_data(payload, self.databases["metrics"])

        print(f"✅ Stock metrics complete: {total_updates} stock updates")

    async def generate_production_events(self, start_time_ms: int, duration_minutes: int):
        """Generate production-related events"""
        print(f"\n📅 Generating production events...")

        times = []
        event_types = []
        severities = []
        descriptions = []
        sources = []
        operators = []

        # Shift changes
        for minute in range(0, duration_minutes, 480):  # Every 8 hours
            timestamp_ms = start_time_ms + (minute * 60 * 1000)
            shift = self.get_current_shift(timestamp_ms)

            times.append(timestamp_ms)
            event_types.append("shift_change")
            severities.append("info")
            descriptions.append(f"Shift change to {shift}")
            sources.append("factory_management")
            operators.append(f"supervisor_{shift}")

        # Daily production targets set
        for day in range(duration_minutes // 1440 + 1):
            timestamp_ms = start_time_ms + (day * 24 * 60 * 60 * 1000)

            times.append(timestamp_ms)
            event_types.append("targets_set")
            severities.append("info")
            descriptions.append(f"Daily production targets configured")
            sources.append("production_planning")
            operators.append("planner_maria")

        # Equipment maintenance
        maintenance_interval = random.randint(240, 360)  # Every 4-6 hours
        for minute in range(0, duration_minutes, maintenance_interval):
            timestamp_ms = start_time_ms + (minute * 60 * 1000)
            equipment = random.choice(list(self.equipment.keys()))

            times.append(timestamp_ms)
            event_types.append("maintenance_completed")
            severities.append("info")
            descriptions.append(f"Preventive maintenance on {equipment}")
            sources.append(equipment)
            operators.append(random.choice(["tech_juan", "tech_pedro", "tech_sofia"]))

        # Quality alerts (occasional)
        for _ in range(duration_minutes // 120):  # Every 2 hours approximately
            timestamp_ms = start_time_ms + random.randint(0, duration_minutes * 60 * 1000)
            batch_id = f"BATCH-{random.randint(1000, 9999)}"

            if random.random() < 0.3:  # 30% chance of quality issue
                times.append(timestamp_ms)
                event_types.append("quality_alert")
                severities.append("warning")
                descriptions.append(f"Quality parameter out of range for {batch_id}")
                sources.append("quality_control")
                operators.append(random.choice(["QC-Maria", "QC-Carlos", "QC-Ana"]))

        # Production milestones
        for _ in range(duration_minutes // 180):  # Every 3 hours
            timestamp_ms = start_time_ms + random.randint(0, duration_minutes * 60 * 1000)
            product = random.choice(list(self.products.keys()))

            times.append(timestamp_ms)
            event_types.append("production_milestone")
            severities.append("info")
            descriptions.append(f"Produced 1000 units of {product}")
            sources.append("production_line")
            operators.append("system")

        # Sales orders received
        for _ in range(duration_minutes // 60):  # Every hour
            timestamp_ms = start_time_ms + random.randint(0, duration_minutes * 60 * 1000)
            order_id = f"ORD-{int(timestamp_ms/1000)}"
            customer = random.choice(["Distributor_A", "Retailer_B", "Export_C", "Wholesale_D"])

            times.append(timestamp_ms)
            event_types.append("sales_order_received")
            severities.append("info")
            descriptions.append(f"New sales order {order_id} from {customer}")
            sources.append("sales_system")
            operators.append("sales_team")

        # Stock alerts
        for _ in range(duration_minutes // 240):  # Every 4 hours
            timestamp_ms = start_time_ms + random.randint(0, duration_minutes * 60 * 1000)
            item = random.choice(list(self.stock.keys()))

            if random.random() < 0.2:  # 20% chance of low stock
                times.append(timestamp_ms)
                event_types.append("stock_alert_low")
                severities.append("warning")
                descriptions.append(f"Low stock alert for {item}: below reorder point")
                sources.append("inventory_system")
                operators.append("system")

        payload = {
            "m": "production_events",
            "columns": {
                "time": times,
                "event_type": event_types,
                "severity": severities,
                "description": descriptions,
                "source": sources,
                "operator": operators
            }
        }

        await self.send_data(payload, self.databases["events"])
        print(f"✅ Production events complete: {len(times)} events")

    async def generate_production_logs(self, start_time_ms: int, duration_minutes: int, batch_size: int = 100):
        """Generate production logs"""
        print(f"\n📝 Generating production logs...")

        total_logs = duration_minutes * 10  # ~10 logs per minute
        batches = (total_logs + batch_size - 1) // batch_size

        log_templates = {
            "info": [
                "Production line {} started batch processing",
                "Equipment {} operating at {}% efficiency",
                "Shift handover completed on line {}",
                "Quality check passed for batch {}",
                "Packaging completed for {} units of product {}"
            ],
            "warning": [
                "Equipment {} temperature approaching upper limit",
                "Production rate on line {} below target by {}%",
                "Quality parameter for batch {} near threshold",
                "Stock level for {} approaching reorder point",
                "Operator {} reported minor issue on {}"
            ],
            "error": [
                "Equipment {} emergency stop triggered",
                "Batch {} rejected - quality test failed",
                "Production line {} halted due to mechanical failure",
                "Packaging error on line {} - {} units affected",
                "Temperature spike detected on equipment {}"
            ]
        }

        for batch_idx in range(batches):
            times = []
            levels = []
            messages = []
            sources = []
            operators = []

            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, total_logs)

            for log_idx in range(batch_start, batch_end):
                timestamp_ms = start_time_ms + random.randint(0, duration_minutes * 60 * 1000)

                # Log level distribution: 70% info, 25% warning, 5% error
                level_choice = random.random()
                if level_choice < 0.70:
                    level = "info"
                elif level_choice < 0.95:
                    level = "warning"
                else:
                    level = "error"

                template = random.choice(log_templates[level])

                # Fill in template - count placeholders and prepare values
                placeholder_count = template.count("{}")

                if placeholder_count > 0:
                    equipment = random.choice(list(self.equipment.keys()))
                    line = random.choice(list(self.production_lines.keys()))
                    product = random.choice(list(self.products.keys()))
                    batch_id = f"BATCH-{random.randint(1000, 9999)}"
                    percentage = random.randint(5, 25)
                    units = random.randint(100, 500)
                    operator = random.choice(["OP-Maria", "OP-Juan", "OP-Carlos", "OP-Sofia"])

                    # Prepare values based on template content
                    values = []
                    for _ in range(placeholder_count):
                        if "equipment" in template.lower() or "Equipment" in template:
                            values.append(equipment)
                        elif "line" in template.lower():
                            values.append(line)
                        elif "batch" in template.lower():
                            values.append(batch_id)
                        elif "product" in template.lower():
                            values.append(product)
                        elif "%" in template:
                            values.append(percentage)
                        elif "units" in template.lower():
                            values.append(units)
                        elif "operator" in template.lower() or "Operator" in template:
                            values.append(operator)
                        else:
                            values.append(random.choice([equipment, line, product]))

                    # Ensure we have enough values
                    while len(values) < placeholder_count:
                        values.append(random.choice([equipment, line, product, batch_id]))

                    message = template.format(*values[:placeholder_count])
                else:
                    message = template

                times.append(timestamp_ms)
                levels.append(level)
                messages.append(message)
                sources.append(random.choice(list(self.equipment.keys())))
                operators.append(random.choice(["OP-Maria", "OP-Juan", "OP-Carlos", "OP-Sofia", "system"]))

            payload = {
                "m": "production_logs",
                "columns": {
                    "time": times,
                    "level": levels,
                    "message": messages,
                    "source": sources,
                    "operator": operators
                }
            }

            await self.send_data(payload, self.databases["logs"])

        print(f"✅ Production logs complete: {total_logs:,} log entries")

    async def generate_batch_traces(self, start_time_ms: int, duration_minutes: int):
        """Generate distributed traces for batch processing through production stages"""
        print(f"\n🔍 Generating batch traces...")

        # Production stages
        stages = [
            ("reception", 5),      # 5 minutes
            ("cleaning", 10),      # 10 minutes
            ("roasting", 30),      # 30 minutes
            ("grinding", 20),      # 20 minutes
            ("conching", 180),     # 3 hours
            ("tempering", 15),     # 15 minutes
            ("molding", 10),       # 10 minutes
            ("packaging", 5),      # 5 minutes
            ("quality_check", 5)   # 5 minutes
        ]

        total_stage_time = sum(duration for _, duration in stages)
        num_batches = duration_minutes // total_stage_time

        all_times = []
        all_trace_ids = []
        all_span_ids = []
        all_parent_span_ids = []
        all_batch_ids = []
        all_stages = []
        all_durations = []
        all_statuses = []
        all_products = []
        all_origins = []

        for batch_num in range(max(1, num_batches)):
            batch_start_ms = start_time_ms + (batch_num * total_stage_time * 60 * 1000)
            trace_id = str(uuid.uuid4())
            batch_id = f"BATCH-{int(batch_start_ms/1000)}-{random.randint(100,999)}"
            product = random.choice(list(self.products.keys()))
            origin = random.choice(self.cacao_origins)

            current_time = batch_start_ms
            parent_span_id = None

            for stage_name, base_duration_min in stages:
                span_id = str(uuid.uuid4())

                # Add some variation to duration
                duration_min = base_duration_min + random.uniform(-base_duration_min * 0.1, base_duration_min * 0.1)
                duration_ms = int(duration_min * 60 * 1000)

                # 95% success rate
                status = "completed" if random.random() < 0.95 else "error"

                all_times.append(current_time)
                all_trace_ids.append(trace_id)
                all_span_ids.append(span_id)
                all_parent_span_ids.append(parent_span_id if parent_span_id else "")
                all_batch_ids.append(batch_id)
                all_stages.append(stage_name)
                all_durations.append(duration_ms)
                all_statuses.append(status)
                all_products.append(product)
                all_origins.append(origin)

                parent_span_id = span_id
                current_time += duration_ms

        payload = {
            "m": "batch_traces",
            "columns": {
                "time": all_times,
                "trace_id": all_trace_ids,
                "span_id": all_span_ids,
                "parent_span_id": all_parent_span_ids,
                "batch_id": all_batch_ids,
                "stage": all_stages,
                "duration_ms": all_durations,
                "status": all_statuses,
                "product": all_products,
                "cacao_origin": all_origins
            }
        }

        await self.send_data(payload, self.databases["traces"])
        print(f"✅ Batch traces complete: {len(all_times)} trace spans across {num_batches} batches")

    async def run_scenario(self, duration_hours: float = 24):
        """Run complete factory scenario"""
        duration_minutes = int(duration_hours * 60)

        # Start from current time minus duration (historical data)
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=duration_hours)
        start_time_ms = int(start_time.timestamp() * 1000)

        print("=" * 80)
        print("F. Pache Cacao Factory - Production Monitoring Demo")
        print("=" * 80)
        print(f"Simulation period: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"Duration: {duration_hours} hours ({duration_minutes} minutes)")
        print(f"Arc URL: {self.arc_url}")
        print(f"Databases: {', '.join(self.databases.values())}")
        print("=" * 80)

        await self.setup()

        try:
            # Generate all data types in parallel
            await asyncio.gather(
                self.generate_production_metrics(start_time_ms, duration_minutes),
                self.generate_quality_metrics(start_time_ms, duration_minutes),
                self.generate_stock_metrics(start_time_ms, duration_minutes),
                self.generate_production_events(start_time_ms, duration_minutes),
                self.generate_production_logs(start_time_ms, duration_minutes),
                self.generate_batch_traces(start_time_ms, duration_minutes)
            )

            print("\n" + "=" * 80)
            print("✅ F. Pache Factory Demo Complete!")
            print("=" * 80)
            print("\nGenerated Data Summary:")
            print(f"  📊 Production metrics: Equipment efficiency, throughput, power")
            print(f"  🔬 Quality metrics: Batch testing, compliance rates")
            print(f"  📦 Stock metrics: Inventory levels, movements, predictions")
            print(f"  📅 Events: Shift changes, maintenance, alerts, orders")
            print(f"  📝 Logs: Equipment status, operator actions, issues")
            print(f"  🔍 Traces: End-to-end batch processing lineage")
            print("\nSample Queries:")
            print(f"  - Production efficiency by shift")
            print(f"  - Stock levels vs. production targets")
            print(f"  - Quality compliance rates by product")
            print(f"  - Batch traceability from bean to bar")
            print(f"  - Predictive stock alerts for sales planning")
            print("=" * 80)

        finally:
            await self.cleanup()


async def main():
    parser = argparse.ArgumentParser(
        description="F. Pache Cacao Factory Production Monitoring Demo"
    )
    parser.add_argument("--url", default="http://localhost:8000", help="Arc API URL")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--database-prefix", default="fpache", help="Database name prefix")
    parser.add_argument("--duration-hours", type=float, default=24, help="Simulation duration in hours")

    args = parser.parse_args()

    scenario = CacaoFactoryScenario(
        arc_url=args.url,
        token=args.token,
        database_prefix=args.database_prefix
    )

    await scenario.run_scenario(duration_hours=args.duration_hours)


if __name__ == "__main__":
    asyncio.run(main())
