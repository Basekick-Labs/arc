# Arc Telemetry

Arc collects anonymous usage telemetry to help us understand how the database is being used and to improve the product. We believe in transparency, so this document explains exactly what data we collect, why we collect it, and how to disable it if you prefer.

## What Data Do We Collect?

Arc sends the following anonymous information to `telemetry.basekick.net` every 24 hours:

```json
{
  "instance_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-10-24T15:30:00Z",
  "arc_version": "25.11",
  "os": {
    "name": "Darwin",
    "version": "23.6.0",
    "architecture": "arm64",
    "platform": "macOS-14.5-arm64-arm-64bit"
  },
  "cpu": {
    "physical_cores": 8,
    "logical_cores": 8,
    "frequency_mhz": 3200.0
  },
  "memory": {
    "total_gb": 16.0
  }
}
```

### Data Points Explained

- **instance_id**: A random UUID generated on first run and stored in `./data/.instance_id`. This is NOT tied to your hardware, IP address, or any personally identifiable information. It simply helps us count unique installations and track version upgrades.

- **timestamp**: When the telemetry report was generated (UTC)

- **arc_version**: The version of Arc you're running (e.g., "25.11")

- **os**: Operating system information
  - `name`: OS name (e.g., "Linux", "Darwin", "Windows")
  - `version`: OS version/kernel version
  - `architecture`: CPU architecture (e.g., "x86_64", "arm64")
  - `platform`: Detailed platform string

- **cpu**: CPU information
  - `physical_cores`: Number of physical CPU cores
  - `logical_cores`: Number of logical CPU cores (with hyperthreading)
  - `frequency_mhz`: Maximum CPU frequency in MHz (if available)

- **memory**: Memory information
  - `total_gb`: Total system RAM in gigabytes

## What We DON'T Collect

Arc telemetry does **NOT** collect:

- ❌ Your data or database contents
- ❌ Database names, measurement names, or field names
- ❌ Query contents or query patterns
- ❌ IP addresses or network information
- ❌ Hostnames or machine names
- ❌ User information or credentials
- ❌ File paths or directory structures
- ❌ Performance metrics or usage statistics
- ❌ Any personally identifiable information (PII)

## Why Do We Collect This?

Understanding the environments where Arc runs helps us:

1. **Prioritize platform support**: Know which operating systems and architectures to focus on
2. **Test on real hardware**: Ensure Arc performs well on the hardware our users actually have
3. **Track adoption**: Understand how many active installations exist
4. **Plan features**: Make informed decisions about which features to build next
5. **Verify upgrades**: Confirm that users are upgrading to new versions

## How to Disable Telemetry

If you prefer to opt out of telemetry, you can disable it in two ways:

### Option 1: Configuration File

Edit `arc.conf` and set `telemetry.enabled` to `false`:

```toml
[telemetry]
enabled = false
```

Then restart Arc:

```bash
# Kill any running Arc instances
pkill -f "python -m api.main"

# Start Arc
./venv/bin/python -m api.main
```

### Option 2: Environment Variable

Set the `ARC_TELEMETRY_ENABLED` environment variable to `false`:

```bash
export ARC_TELEMETRY_ENABLED=false
./venv/bin/python -m api.main
```

## Telemetry Schedule

- **First Send**: 1 minute after Arc starts (to ensure Arc is fully initialized)
- **Subsequent Sends**: Every 24 hours after the first send
- **On Startup**: Telemetry is only sent from the primary worker process (not all workers)

## Verifying Telemetry Status

When Arc starts, you'll see a log message indicating whether telemetry is enabled:

```
Telemetry enabled: sending to https://telemetry.basekick.net/api/v1/telemetry every 24h
Instance ID: a1b2c3d4... (to disable: set telemetry.enabled=false in arc.conf)
```

Or if disabled:

```
Telemetry disabled by configuration
```

## Network Requirements

If telemetry is enabled, Arc will make HTTPS POST requests to:

```
https://telemetry.basekick.net/api/v1/telemetry
```

If this endpoint is unreachable (due to firewall, no internet, etc.), Arc will:
- Log a warning message
- Continue operating normally (telemetry failures do not affect Arc's functionality)
- Retry on the next scheduled send (24 hours later)

## Privacy & Transparency

We take privacy seriously:

- All telemetry data is sent over HTTPS
- The telemetry payload is human-readable JSON (not obfuscated or encrypted beyond HTTPS)
- You can inspect exactly what's being sent by checking the Arc startup logs
- The telemetry server is operated by Basekick Labs
- We do not share, sell, or distribute telemetry data to third parties

## Questions?

If you have questions about telemetry, please:

1. Open an issue: https://github.com/Basekick-Labs/arc/issues
2. Email us: support@basekick.net
3. Review the source code: `telemetry/collector.py` and `telemetry/sender.py`

We're committed to transparency and happy to answer any questions about our data collection practices.
