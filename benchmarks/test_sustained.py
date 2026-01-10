#!/usr/bin/env python3
"""Sustained load test - runs for specified duration"""
import msgpack
import aiohttp
import asyncio
import time
import random
import argparse
import gzip
import os
import ssl

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

try:
    from aioquic.asyncio.client import connect as quic_connect
    from aioquic.asyncio.protocol import QuicConnectionProtocol
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.h3.connection import H3_ALPN, H3Connection
    from aioquic.h3.events import HeadersReceived, DataReceived
    HTTP3_AVAILABLE = True
except ImportError:
    HTTP3_AVAILABLE = False

class SustainedLoadTester:
    def __init__(self, url, batch_size, pregenerated_batches, compression="none", token=None):
        self.url = url
        self.batch_size = batch_size
        self.batches = pregenerated_batches  # Pre-generated MessagePack batches
        self.compression = compression  # "none", "gzip", or "zstd"
        self.token = token
        self.batch_index = 0
        self.total_sent = 0
        self.total_errors = 0
        self.latencies = []
        self.lock = asyncio.Lock()
        self.running = True

    async def send_batch(self, session):
        """Send a single pre-generated batch"""
        start = time.perf_counter()

        try:
            # Get pre-generated batch (cycle through them)
            batch_data = self.batches[self.batch_index % len(self.batches)]
            self.batch_index += 1

            headers = {
                "Content-Type": "application/msgpack",
                "x-arc-database": "production"
            }

            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            # Note: Content-Encoding header is not needed - Arc auto-detects by magic bytes
            # But we set it for clarity/debugging
            if self.compression == "gzip":
                headers["Content-Encoding"] = "gzip"
            elif self.compression == "zstd":
                headers["Content-Encoding"] = "zstd"

            async with session.post(self.url, data=batch_data, headers=headers) as response:
                latency = (time.perf_counter() - start) * 1000

                async with self.lock:
                    if response.status == 204:
                        self.total_sent += self.batch_size
                        self.latencies.append(latency)
                    else:
                        self.total_errors += 1
                        if self.total_errors <= 3:
                            text = await response.text()
                            print(f"Error {response.status}: {text[:100]}")
        except Exception as e:
            async with self.lock:
                self.total_errors += 1
                if self.total_errors <= 3:
                    print(f"Exception: {e}")

    async def worker(self, session, worker_id):
        """Worker that continuously sends batches"""
        while self.running:
            await self.send_batch(session)

    def get_percentile(self, p):
        if not self.latencies:
            return 0.0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * p)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]


# =============================================================================
# HTTP/3 Support
# =============================================================================
if HTTP3_AVAILABLE:
    class H3ClientProtocol(QuicConnectionProtocol):
        """HTTP/3 client protocol handler."""

        def __init__(self, *args, authority: str = "", **kwargs):
            super().__init__(*args, **kwargs)
            self._http = None
            self._request_events = {}
            self._request_waiters = {}
            self._authority = authority

        def http_connection_ready(self):
            self._http = H3Connection(self._quic)

        async def send_request(self, path: str, data: bytes, headers: dict):
            stream_id = self._quic.get_next_available_stream_id()

            request_headers = [
                (b":method", b"POST"),
                (b":scheme", b"https"),
                (b":authority", self._authority.encode()),
                (b":path", path.encode()),
            ]
            for key, value in headers.items():
                request_headers.append((key.lower().encode(), str(value).encode()))

            self._http.send_headers(stream_id, request_headers)
            self._http.send_data(stream_id, data, end_stream=True)

            waiter = asyncio.get_event_loop().create_future()
            self._request_waiters[stream_id] = waiter
            self._request_events[stream_id] = {"status": 0, "data": b""}

            self.transmit()

            try:
                await asyncio.wait_for(waiter, timeout=30.0)
            except asyncio.TimeoutError:
                self._request_events.pop(stream_id, None)
                self._request_waiters.pop(stream_id, None)
                raise

            result = self._request_events.pop(stream_id)
            return result["status"], result["data"]

        def quic_event_received(self, event):
            if self._http is not None:
                for http_event in self._http.handle_event(event):
                    self._http_event_received(http_event)

        def _http_event_received(self, event):
            if isinstance(event, HeadersReceived):
                stream_id = event.stream_id
                if stream_id in self._request_events:
                    for header, value in event.headers:
                        if header == b":status":
                            self._request_events[stream_id]["status"] = int(value.decode())
                    if event.stream_ended and stream_id in self._request_waiters:
                        self._request_waiters[stream_id].set_result(None)
                        del self._request_waiters[stream_id]
            elif isinstance(event, DataReceived):
                stream_id = event.stream_id
                if stream_id in self._request_events:
                    self._request_events[stream_id]["data"] += event.data
                    if event.stream_ended and stream_id in self._request_waiters:
                        self._request_waiters[stream_id].set_result(None)
                        del self._request_waiters[stream_id]


    class HTTP3LoadTester:
        """HTTP/3 load tester using aioquic."""

        def __init__(self, host, port, batch_size, pregenerated_batches, compression="none", token=None):
            self.host = host
            self.port = port
            self.batch_size = batch_size
            self.batches = pregenerated_batches
            self.compression = compression
            self.token = token
            self.batch_index = 0
            self.total_sent = 0
            self.total_errors = 0
            self.latencies = []
            self.lock = asyncio.Lock()
            self.running = True

        def get_headers(self):
            headers = {
                "content-type": "application/msgpack",
                "x-arc-database": "production"
            }
            if self.token:
                headers["authorization"] = f"Bearer {self.token}"
            if self.compression == "gzip":
                headers["content-encoding"] = "gzip"
            elif self.compression == "zstd":
                headers["content-encoding"] = "zstd"
            return headers

        async def send_batch(self, client):
            start = time.perf_counter()
            try:
                batch_data = self.batches[self.batch_index % len(self.batches)]
                self.batch_index += 1

                status, _ = await client.send_request(
                    "/api/v1/write/msgpack",
                    batch_data,
                    self.get_headers()
                )
                latency = (time.perf_counter() - start) * 1000

                async with self.lock:
                    if status == 204:
                        self.total_sent += self.batch_size
                        self.latencies.append(latency)
                    else:
                        self.total_errors += 1
                        if self.total_errors <= 3:
                            print(f"HTTP/3 Error {status}")
            except Exception as e:
                async with self.lock:
                    self.total_errors += 1
                    if self.total_errors <= 3:
                        print(f"HTTP/3 Exception: {e}")

        async def worker(self, client, worker_id):
            while self.running:
                await self.send_batch(client)

        def get_percentile(self, p):
            if not self.latencies:
                return 0.0
            sorted_lat = sorted(self.latencies)
            idx = int(len(sorted_lat) * p)
            return sorted_lat[min(idx, len(sorted_lat) - 1)]


async def main():
    parser = argparse.ArgumentParser(description="Sustained load test")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")
    parser.add_argument("--workers", type=int, default=100, help="Number of concurrent workers")
    parser.add_argument("--batch-size", type=int, default=1000, help="Records per batch")
    parser.add_argument("--pregenerate", type=int, default=1000, help="Number of batches to pre-generate")
    parser.add_argument("--compress", type=str, default="none", choices=["none", "gzip", "zstd"],
                        help="Compression: 'none', 'gzip', or 'zstd' (recommended)")
    parser.add_argument("--zstd-level", type=int, default=3, choices=range(1, 23),
                        help="Zstd compression level (1=fastest, 22=best compression, default=3)")
    parser.add_argument("--data-type", type=str, default="iot", choices=["iot", "financial"],
                        help="Data type: 'iot' (server metrics) or 'financial' (stock prices)")
    parser.add_argument("--tls", action="store_true", help="Use HTTPS (TLS)")
    parser.add_argument("--http3", action="store_true", help="Use HTTP/3 (QUIC) - requires TLS")
    parser.add_argument("--unix-socket", type=str, default="", help="Unix socket path (e.g., /tmp/arc.sock)")
    parser.add_argument("--host", type=str, default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=8000, help="Server port")
    args = parser.parse_args()

    # HTTP/3 validation
    if args.http3:
        if not HTTP3_AVAILABLE:
            print("ERROR: HTTP/3 requested but 'aioquic' package not installed.")
            print("Install with: pip install aioquic")
            return
        args.tls = True  # HTTP/3 requires TLS

    # Unix socket validation
    if args.unix_socket:
        if args.http3:
            print("ERROR: Unix socket cannot be used with HTTP/3")
            return
        if args.tls:
            print("ERROR: Unix socket cannot be used with TLS")
            return

    scheme = "https" if args.tls else "http"
    if args.unix_socket:
        url = "http://localhost/api/v1/write/msgpack"  # Host doesn't matter for Unix socket
    else:
        url = f"{scheme}://{args.host}:{args.port}/api/v1/write/msgpack"

    # Validate zstd availability
    if args.compress == "zstd" and not ZSTD_AVAILABLE:
        print("ERROR: zstd compression requested but 'zstandard' package not installed.")
        print("Install with: pip install zstandard")
        return

    compression_labels = {
        "none": "No Compression",
        "gzip": "WITH GZIP COMPRESSION",
        "zstd": f"WITH ZSTD COMPRESSION (level={args.zstd_level})"
    }
    compression_label = compression_labels[args.compress]
    data_type_label = "FINANCIAL (Stock Prices)" if args.data_type == "financial" else "IOT (Server Metrics)"
    if args.unix_socket:
        tls_label = f"Unix Socket ({args.unix_socket})"
    elif args.http3:
        tls_label = "HTTP/3 (QUIC)"
    elif args.tls:
        tls_label = "HTTPS (TLS)"
    else:
        tls_label = "HTTP"
    print("="*80)
    print(f"SUSTAINED LOAD TEST ({compression_label})")
    print("="*80)
    print(f"Target: {url}")
    print(f"Protocol: {tls_label}")
    print(f"Data type: {data_type_label}")
    print(f"Duration: {args.duration}s")
    print(f"Batch size: {args.batch_size:,}")
    print(f"Workers: {args.workers}")
    print(f"Pre-generate: {args.pregenerate:,} batches")
    print(f"Mode: CONTINUOUS (max throughput)")
    print("="*80)
    print()

    # Pre-generate batches (eliminate client-side overhead)
    print(f"Pre-generating {args.pregenerate:,} batches...")
    start_gen = time.time()

    batches = []

    if args.data_type == "financial":
        # Financial data: Stock market tick data
        # Typical OHLCV + bid/ask data for equities
        symbols = [
            "AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
            "WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX",
            "CRM", "CMCSA", "XOM", "VZ", "INTC", "T", "PFE", "ABT", "KO", "PEP",
            "MRK", "TMO", "CSCO", "AVGO", "ACN", "NKE", "COST", "DHR", "LLY", "MDT",
            "NEE", "TXN", "QCOM", "HON", "UNP", "PM", "LOW", "AMGN", "IBM", "ORCL"
        ]
        exchanges = ["NYSE", "NASDAQ", "ARCA", "BATS", "IEX"]

        for i in range(args.pregenerate):
            now_micros = int(time.time() * 1_000_000)

            # Generate realistic stock prices (between $10 and $500)
            base_prices = [random.uniform(10, 500) for _ in range(args.batch_size)]

            payload = {
                "m": "trades",
                "columns": {
                    "time": [now_micros + j for j in range(args.batch_size)],
                    "symbol": [random.choice(symbols) for _ in range(args.batch_size)],
                    "exchange": [random.choice(exchanges) for _ in range(args.batch_size)],
                    "price": base_prices,
                    "bid": [p - random.uniform(0.01, 0.05) for p in base_prices],
                    "ask": [p + random.uniform(0.01, 0.05) for p in base_prices],
                    "bid_size": [random.randint(100, 10000) for _ in range(args.batch_size)],
                    "ask_size": [random.randint(100, 10000) for _ in range(args.batch_size)],
                    "volume": [random.randint(1, 1000) for _ in range(args.batch_size)],
                    "trade_id": [random.randint(1000000, 9999999) for _ in range(args.batch_size)],
                }
            }

            msgpack_data = msgpack.packb(payload)

            if args.compress == "gzip":
                msgpack_data = gzip.compress(msgpack_data, compresslevel=1)
            elif args.compress == "zstd":
                cctx = zstd.ZstdCompressor(level=args.zstd_level)
                msgpack_data = cctx.compress(msgpack_data)

            batches.append(msgpack_data)

            if (i + 1) % 100 == 0:
                print(f"  Progress: {i+1}/{args.pregenerate}")
    else:
        # IOT data: Server metrics (original)
        measurements = ["cpu", "mem", "disk", "net"]
        hosts = [f"server{i:03d}" for i in range(1000)]

        for i in range(args.pregenerate):
            now_micros = int(time.time() * 1_000_000)

            payload = {
                "m": random.choice(measurements),
                "columns": {
                    "time": [now_micros + j for j in range(args.batch_size)],
                    "host": [random.choice(hosts) for _ in range(args.batch_size)],
                    "value": [random.random() * 100 for _ in range(args.batch_size)],
                    "cpu_idle": [random.random() * 100 for _ in range(args.batch_size)],
                    "cpu_user": [random.random() * 100 for _ in range(args.batch_size)],
                }
            }

            msgpack_data = msgpack.packb(payload)

            # Apply compression if enabled
            if args.compress == "gzip":
                msgpack_data = gzip.compress(msgpack_data, compresslevel=1)
            elif args.compress == "zstd":
                cctx = zstd.ZstdCompressor(level=args.zstd_level)
                msgpack_data = cctx.compress(msgpack_data)

            batches.append(msgpack_data)

            if (i + 1) % 100 == 0:
                print(f"  Progress: {i+1}/{args.pregenerate}")

    gen_time = time.time() - start_gen
    avg_size = sum(len(b) for b in batches) / len(batches)
    size_label = args.compress if args.compress != "none" else "uncompressed"
    print(f"✅ Generated {args.pregenerate:,} batches in {gen_time:.1f}s")
    print(f"   Avg size: {avg_size/1024:.1f} KB ({size_label})")
    print()

    # Get token from environment
    token = os.environ.get("ARC_TOKEN")
    if token:
        print(f"Using auth token: {token[:8]}...")
    else:
        print("No ARC_TOKEN set - authentication may fail")

    # Run test
    print("Starting test...")
    start = time.time()

    if args.http3:
        # HTTP/3 test using aioquic
        tester = HTTP3LoadTester(args.host, args.port, args.batch_size, batches,
                                  compression=args.compress, token=token)

        configuration = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
        configuration.verify_mode = ssl.CERT_NONE

        authority = f"{args.host}:{args.port}"

        def create_protocol(*protocol_args, **protocol_kwargs):
            return H3ClientProtocol(*protocol_args, authority=authority, **protocol_kwargs)

        try:
            async with quic_connect(
                args.host,
                args.port,
                configuration=configuration,
                create_protocol=create_protocol,
            ) as client:
                client.http_connection_ready()

                # HTTP/3 multiplexes on single connection
                effective_workers = min(args.workers, 20)
                workers = [asyncio.create_task(tester.worker(client, i)) for i in range(effective_workers)]

                last_sent = 0
                while time.time() - start < args.duration:
                    await asyncio.sleep(5)
                    elapsed = time.time() - start
                    interval_rps = (tester.total_sent - last_sent) / 5
                    print(f"[{elapsed:6.1f}s] RPS: {int(interval_rps):>10,} | Total: {tester.total_sent:>12,} | Errors: {tester.total_errors:>6}")
                    last_sent = tester.total_sent

                tester.running = False
                await asyncio.gather(*workers, return_exceptions=True)
        except Exception as e:
            print(f"HTTP/3 connection failed: {e}")
            return
    else:
        # HTTP/1.1, HTTPS, or Unix socket test using aiohttp
        tester = SustainedLoadTester(url, args.batch_size, batches, compression=args.compress, token=token)

        ssl_context = None
        if args.tls:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        if args.unix_socket:
            # Unix socket connector - bypasses TCP/IP for same-machine connections
            connector = aiohttp.UnixConnector(
                path=args.unix_socket,
                limit=150,
                limit_per_host=150,
                force_close=False,
                keepalive_timeout=30,
            )
        else:
            connector = aiohttp.TCPConnector(
                limit=150,
                limit_per_host=150,
                ttl_dns_cache=300,
                force_close=False,
                enable_cleanup_closed=True,
                keepalive_timeout=30,
                ssl=ssl_context
            )
        timeout = aiohttp.ClientTimeout(total=120, connect=10, sock_read=10)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            workers = [
                asyncio.create_task(tester.worker(session, i))
                for i in range(args.workers)
            ]

            last_sent = 0
            while time.time() - start < args.duration:
                await asyncio.sleep(5)
                elapsed = time.time() - start
                interval_rps = (tester.total_sent - last_sent) / 5
                print(f"[{elapsed:6.1f}s] RPS: {int(interval_rps):>10,} | Total: {tester.total_sent:>12,} | Errors: {tester.total_errors:>6}")
                last_sent = tester.total_sent

            tester.running = False
            await asyncio.gather(*workers, return_exceptions=True)

    elapsed = time.time() - start

    # Results
    print()
    print("="*80)
    print("RESULTS")
    print("="*80)
    print(f"Duration:        {elapsed:.1f}s")
    print(f"Total sent:      {tester.total_sent:,} records")
    print(f"Total errors:    {tester.total_errors}")
    print(f"Success rate:    {(tester.total_sent/(tester.total_sent+max(tester.total_errors,1))*100):.2f}%")
    print(f"")
    print(f"🚀 THROUGHPUT:   {int(tester.total_sent/elapsed):,} records/sec")
    print(f"")
    print(f"Latency percentiles:")
    print(f"  p50:  {tester.get_percentile(0.50):.2f} ms")
    print(f"  p95:  {tester.get_percentile(0.95):.2f} ms")
    print(f"  p99:  {tester.get_percentile(0.99):.2f} ms")
    print(f"  p999: {tester.get_percentile(0.999):.2f} ms")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())
