# F. Pache Cacao Factory Demo - Grafana Dashboard Guide

This demo simulates a complete cacao processing factory operation for F. Pache (https://fpache.com), generating realistic observability data across metrics, logs, traces, and events.

## Quick Start

```bash
# Generate 24 hours of factory data
./demo_cacao_factory.py --token YOUR_TOKEN --duration-hours 24

# For production server
./demo_cacao_factory.py \
  --url http://h01-customers-basekick-net:8000 \
  --token YOUR_TOKEN \
  --duration-hours 48
```

## Databases Created

- `fpache_metrics` - Production metrics, quality metrics, stock levels
- `fpache_logs` - Equipment logs, operator actions
- `fpache_traces` - Batch processing traces (bean to bar)
- `fpache_events` - Business events (orders, alerts, maintenance)

## Production Chain Simulated

```
Raw Cacao Beans → Cleaning → Roasting → Grinding →
Conching → Tempering → Molding → Packaging → QC → Warehouse
```

## Grafana Dashboard Queries

### 1. Production Overview Dashboard

#### Panel: Production Efficiency by Line (Time Series)
```sql
SELECT
  time_bucket(INTERVAL '5 minutes', time) as time,
  line_id,
  AVG(value) as avg_efficiency
FROM fpache_metrics.production_metrics
WHERE
  metric_name = 'efficiency'
  AND $__timeFilter(time)
GROUP BY time_bucket(INTERVAL '5 minutes', time), line_id
ORDER BY time ASC
```

#### Panel: Equipment Status (Stat)
```sql
SELECT
  equipment_id,
  LAST(status ORDER BY time) as status,
  LAST(value ORDER BY time) as current_efficiency
FROM fpache_metrics.production_metrics
WHERE
  metric_name = 'efficiency'
  AND $__timeFilter(time)
GROUP BY equipment_id
ORDER BY equipment_id
```

#### Panel: Real-time Throughput (Time Series)
```sql
SELECT
  time,
  equipment_id,
  value as throughput
FROM fpache_metrics.production_metrics
WHERE
  metric_name = 'throughput'
  AND $__timeFilter(time)
ORDER BY time ASC
LIMIT 1000
```

#### Panel: Power Consumption by Equipment (Time Series)
```sql
SELECT
  time_bucket(INTERVAL '15 minutes', time) as time,
  equipment_id,
  AVG(value) as avg_power_kw
FROM fpache_metrics.production_metrics
WHERE
  metric_name = 'power_consumption'
  AND $__timeFilter(time)
GROUP BY time_bucket(INTERVAL '15 minutes', time), equipment_id
ORDER BY time ASC
```

### 2. Quality Control Dashboard

#### Panel: Quality Test Pass Rate (Gauge)
```sql
SELECT
  COUNT(CASE WHEN result = 'pass' THEN 1 END) * 100.0 / COUNT(*) as pass_rate
FROM fpache_metrics.quality_metrics
WHERE $__timeFilter(time)
```

#### Panel: Quality Tests Over Time (Time Series)
```sql
SELECT
  time_bucket(INTERVAL '30 minutes', time) as time,
  test_name,
  COUNT(*) as test_count,
  SUM(CASE WHEN result = 'fail' THEN 1 ELSE 0 END)::BIGINT as failures
FROM fpache_metrics.quality_metrics
WHERE $__timeFilter(time)
GROUP BY time_bucket(INTERVAL '30 minutes', time), test_name
ORDER BY time ASC
```

#### Panel: Failed Batches (Table)
```sql
SELECT
  time,
  batch_id,
  product,
  test_name,
  value,
  inspector
FROM fpache_metrics.quality_metrics
WHERE
  result = 'fail'
  AND $__timeFilter(time)
ORDER BY time DESC
LIMIT 50
```

#### Panel: Quality by Product (Bar Chart)
```sql
SELECT
  product,
  COUNT(*) as total_tests,
  COUNT(CASE WHEN result = 'pass' THEN 1 END)::BIGINT as passed,
  COUNT(CASE WHEN result = 'fail' THEN 1 END)::BIGINT as failed,
  COUNT(CASE WHEN result = 'pass' THEN 1 END) * 100.0 / COUNT(*) as pass_rate
FROM fpache_metrics.quality_metrics
WHERE $__timeFilter(time)
GROUP BY product
ORDER BY pass_rate DESC
```

### 3. Inventory & Stock Dashboard

#### Panel: Current Stock Levels (Bar Gauge)
```sql
SELECT
  item,
  LAST(quantity ORDER BY time) as current_stock,
  type
FROM fpache_metrics.stock_levels
WHERE $__timeFilter(time)
GROUP BY item, type
ORDER BY current_stock ASC
```

#### Panel: Stock Movement Trends (Time Series)
```sql
SELECT
  time,
  item,
  quantity
FROM fpache_metrics.stock_levels
WHERE $__timeFilter(time)
ORDER BY time ASC
```

#### Panel: Raw Materials Stock (Stat Panel)
```sql
SELECT
  item,
  LAST(quantity ORDER BY time) as kg_available
FROM fpache_metrics.stock_levels
WHERE
  type = 'raw_material'
  AND $__timeFilter(time)
GROUP BY item
```

#### Panel: Low Stock Alerts (Table)
```sql
SELECT
  item,
  LAST(quantity ORDER BY time) as current_quantity,
  location,
  LAST(time ORDER BY time) as last_checked
FROM fpache_metrics.stock_levels
WHERE
  $__timeFilter(time)
GROUP BY item, location
HAVING LAST(quantity ORDER BY time) < 1000  -- Threshold
ORDER BY current_quantity ASC
```

#### Panel: Stock Consumption Rate (Time Series)
```sql
SELECT
  time_bucket(INTERVAL '1 hour', time) as time,
  item,
  AVG(movement) as avg_movement
FROM fpache_metrics.stock_levels
WHERE
  type = 'raw_material'
  AND $__timeFilter(time)
GROUP BY time_bucket(INTERVAL '1 hour', time), item
ORDER BY time ASC
```

### 4. Production vs. Targets Dashboard

#### Panel: Production vs Stock Levels (Bar Chart)
```sql
-- Show current stock levels for finished products as proxy for production
SELECT
  item as product,
  LAST(quantity ORDER BY time) as current_stock,
  -- Target would be defined per product in a real scenario
  CASE
    WHEN item LIKE 'bar_%' THEN 2000
    WHEN item = 'chips_dark' THEN 1500
    WHEN item = 'cocoa_powder' THEN 1000
    WHEN item = 'cocoa_butter' THEN 800
    WHEN item = 'nibs' THEN 1200
    ELSE 1000
  END as target_stock,
  (LAST(quantity ORDER BY time) * 100.0 /
    CASE
      WHEN item LIKE 'bar_%' THEN 2000
      WHEN item = 'chips_dark' THEN 1500
      WHEN item = 'cocoa_powder' THEN 1000
      WHEN item = 'cocoa_butter' THEN 800
      WHEN item = 'nibs' THEN 1200
      ELSE 1000
    END
  ) as stock_achievement_pct
FROM fpache_metrics.stock_levels
WHERE
  type = 'finished_product'
  AND $__timeFilter(time)
GROUP BY item
ORDER BY stock_achievement_pct DESC
```

#### Panel: Shift Performance (Time Series)
```sql
SELECT
  time_bucket(INTERVAL '8 hours', time) as shift_time,
  AVG(value) as avg_efficiency
FROM fpache_metrics.production_metrics
WHERE
  metric_name = 'efficiency'
  AND $__timeFilter(time)
GROUP BY time_bucket(INTERVAL '8 hours', time)
ORDER BY shift_time ASC
```

### 5. Batch Traceability Dashboard

#### Panel: Batch Processing Timeline (Gantt/Table)
```sql
SELECT
  batch_id,
  stage,
  cacao_origin,
  product,
  MIN(time) as stage_start,
  duration_ms / 1000.0 / 60 as duration_minutes,
  status
FROM fpache_traces.batch_traces
WHERE $__timeFilter(time)
GROUP BY batch_id, stage, cacao_origin, product, duration_ms, status
ORDER BY batch_id, stage_start
LIMIT 100
```

#### Panel: Average Processing Time by Stage (Bar Chart)
```sql
SELECT
  stage,
  AVG(duration_ms) / 1000.0 / 60 as avg_duration_minutes,
  COUNT(*) as batch_count
FROM fpache_traces.batch_traces
WHERE $__timeFilter(time)
GROUP BY stage
ORDER BY avg_duration_minutes DESC
```

#### Panel: Batch Success Rate (Stat)
```sql
SELECT
  COUNT(CASE WHEN status = 'completed' THEN 1 END) * 100.0 / COUNT(*) as success_rate
FROM fpache_traces.batch_traces
WHERE
  $__timeFilter(time)
  AND stage = 'quality_check'  -- Final stage
```

#### Panel: End-to-End Batch Time (Time Series)
```sql
SELECT
  MIN(time) as batch_start,
  batch_id,
  (SUM(duration_ms) / 1000.0 / 60)::DOUBLE as total_time_minutes,
  FIRST(cacao_origin ORDER BY time) as origin
FROM fpache_traces.batch_traces
WHERE $__timeFilter(time)
GROUP BY batch_id
ORDER BY batch_start ASC
```

### 6. Events & Alerts Dashboard

#### Panel: Production Events Timeline (Logs Panel)
```sql
SELECT
  time,
  event_type,
  severity,
  description,
  source,
  operator
FROM fpache_events.production_events
WHERE $__timeFilter(time)
ORDER BY time DESC
LIMIT 200
```

#### Panel: Events by Type (Pie Chart)
```sql
SELECT
  event_type,
  COUNT(*) as event_count
FROM fpache_events.production_events
WHERE $__timeFilter(time)
GROUP BY event_type
ORDER BY event_count DESC
```

#### Panel: Critical Events (Table)
```sql
SELECT
  time,
  event_type,
  description,
  source,
  operator
FROM fpache_events.production_events
WHERE
  severity IN ('error', 'critical', 'warning')
  AND $__timeFilter(time)
ORDER BY time DESC
LIMIT 50
```

#### Panel: Sales Orders Today (Stat)
```sql
SELECT
  COUNT(*) as order_count
FROM fpache_events.production_events
WHERE
  event_type = 'sales_order_received'
  AND $__timeFilter(time)
```

### 7. Operational Logs Dashboard

#### Panel: Recent Logs (Logs Panel in Grafana)
```sql
SELECT
  time,
  level,
  message,
  source,
  operator
FROM fpache_logs.production_logs
WHERE $__timeFilter(time)
ORDER BY time DESC
LIMIT 500
```

#### Panel: Errors and Warnings (Time Series)
```sql
SELECT
  time_bucket(INTERVAL '15 minutes', time) as bucket_time,
  level,
  COUNT(*) as log_count
FROM fpache_logs.production_logs
WHERE
  level IN ('warning', 'error')
  AND $__timeFilter(time)
GROUP BY time_bucket(INTERVAL '15 minutes', time), level
ORDER BY bucket_time ASC
```

#### Panel: Top Error Sources (Bar Chart)
```sql
SELECT
  source,
  COUNT(*) as error_count
FROM fpache_logs.production_logs
WHERE
  level = 'error'
  AND $__timeFilter(time)
GROUP BY source
ORDER BY error_count DESC
LIMIT 10
```

### 8. Predictive Analytics Queries

#### Panel: Stock Depletion Forecast (Table)
```sql
-- Calculate consumption rate and predict when stock will hit zero
-- Note: This is a simplified forecast based on recent consumption patterns
SELECT
  item,
  LAST(quantity ORDER BY time) as current_qty,
  AVG(CASE WHEN movement < 0 THEN movement ELSE NULL END) as avg_consumption_rate,
  -- Estimate hours until depleted (stock checked every 5 minutes)
  CASE
    WHEN AVG(CASE WHEN movement < 0 THEN movement ELSE NULL END) < 0 THEN
      (LAST(quantity ORDER BY time) / ABS(AVG(CASE WHEN movement < 0 THEN movement ELSE NULL END))) * 0.0833
    ELSE NULL
  END as hours_until_depleted
FROM fpache_metrics.stock_levels
WHERE
  type = 'raw_material'
  AND $__timeFilter(time)
GROUP BY item
HAVING AVG(CASE WHEN movement < 0 THEN movement ELSE NULL END) IS NOT NULL
ORDER BY hours_until_depleted ASC NULLS LAST
```

#### Panel: Production Capacity Utilization (Gauge)
```sql
SELECT
  equipment_id,
  AVG(value) as avg_utilization
FROM fpache_metrics.production_metrics
WHERE
  metric_name = 'efficiency'
  AND $__timeFilter(time)
GROUP BY equipment_id
ORDER BY avg_utilization DESC
```

## Business Intelligence Queries

### Sales vs. Production Gap Analysis
```sql
-- Requires combining stock movement with production events
SELECT
  item,
  SUM(CASE WHEN movement > 0 THEN movement ELSE 0 END)::DOUBLE as total_produced,
  ABS(SUM(CASE WHEN movement < 0 THEN movement ELSE 0 END))::DOUBLE as total_sold,
  SUM(movement)::DOUBLE as net_change
FROM fpache_metrics.stock_levels
WHERE
  type = 'finished_product'
  AND $__timeFilter(time)
GROUP BY item
ORDER BY net_change ASC
```

### OEE (Overall Equipment Effectiveness)
```sql
-- OEE = Availability × Performance × Quality
SELECT
  equipment_id,
  AVG(CASE WHEN status = 'running' THEN 1.0 ELSE 0.0 END) as availability,
  AVG(value) / 100.0 as performance,
  0.95::DOUBLE as quality,  -- From quality_metrics in real scenario
  (AVG(CASE WHEN status = 'running' THEN 1.0 ELSE 0.0 END) *
   AVG(value) / 100.0 *
   0.95) * 100 as oee_percent
FROM fpache_metrics.production_metrics
WHERE
  metric_name = 'efficiency'
  AND $__timeFilter(time)
GROUP BY equipment_id
ORDER BY oee_percent DESC
```

## Demo Scenario Timeline

The demo generates data simulating:

1. **Normal Operations** (0-8 hours)
   - Steady production on all lines
   - Regular quality checks
   - Stock consumption and replenishment

2. **Morning Shift Peak** (8-12 hours)
   - Highest efficiency (92%)
   - Maximum throughput
   - Minimal errors

3. **Afternoon Shift** (12-20 hours)
   - Good efficiency (88%)
   - Some quality alerts
   - Sales orders processing

4. **Night Shift** (20-24 hours)
   - Lower efficiency (82%)
   - Maintenance activities
   - Stock level checks

## Key Insights to Demonstrate

1. **Production Efficiency**: Show how different shifts affect output
2. **Quality Correlation**: Link quality failures to specific equipment or batches
3. **Stock Prediction**: Demonstrate when raw materials need reordering
4. **Batch Traceability**: Track a chocolate bar back to its origin cacao beans
5. **Real-time Alerts**: Show stock alerts, quality issues, equipment problems
6. **Business Impact**: Connect production metrics to sales capacity

## Tips for Best Demo Experience

1. Use **24-48 hours** of data for meaningful trends
2. Create **multiple dashboards** for different stakeholder views:
   - Operations Dashboard (for factory floor)
   - Quality Dashboard (for QC team)
   - Business Dashboard (for management)
3. Set up **alerts** in Grafana for:
   - Stock levels below threshold
   - Quality test failures
   - Equipment efficiency drops
4. Use **variables** in Grafana for:
   - Production line selection
   - Product type filter
   - Shift selection
5. Combine **logs + metrics + traces** to show unified observability

## F. Pache Context

F. Pache is a real cacao processor in Uruguay (https://fpache.com) specializing in:
- **Origins**: Cacao del Plata (Río de la Plata region), Misiones, various South American origins
- **Products**: Chocolate bars (70%, 85%, 100%), cocoa powder, chips, nibs, butter
- **Scale**: Industrial processing with quality focus
- **Market**: B2B and retail chocolate products across South America

This demo showcases how Arc can provide comprehensive monitoring for specialty food manufacturing where traceability, quality, and production efficiency are critical.
