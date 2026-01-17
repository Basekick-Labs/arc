/*
 * Arc Sustained Load Benchmark - C Client
 *
 * A high-performance HTTP benchmark client using libcurl and msgpack.
 * Uses the same MessagePack columnar format as Arc's /api/v1/write/msgpack endpoint.
 *
 * Build:
 *   make -C benchmarks/c_bench
 *
 * Usage:
 *   ./benchmarks/c_bench/sustained_bench -d 30 -w 100 -b 1000
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <getopt.h>
#include <curl/curl.h>
#include <stdatomic.h>
#include <msgpack.h>

/* Configuration */
typedef struct {
    int duration;
    int workers;
    int batch_size;
    int pregenerate;
    char *host;
    int port;
    char *token;
    char *data_type;  /* "iot" or "financial" */
} config_t;

/* Pre-generated batch */
typedef struct {
    char *data;
    size_t size;
} batch_t;

/* Statistics (lock-free atomics) */
typedef struct {
    atomic_long total_sent;
    atomic_long total_errors;
    atomic_bool running;

    /* Latency tracking */
    double *latencies;
    atomic_long latency_count;
    size_t latency_capacity;
    pthread_mutex_t latency_mutex;
} stats_t;

/* Worker context */
typedef struct {
    int id;
    config_t *cfg;
    batch_t *batches;
    int batch_count;
    stats_t *stats;
    CURL *curl;
    struct curl_slist *headers;
    char url[256];
} worker_ctx_t;

/* Global config defaults */
static config_t default_config = {
    .duration = 60,
    .workers = 100,
    .batch_size = 1000,
    .pregenerate = 1000,
    .host = "localhost",
    .port = 8000,
    .token = NULL,
    .data_type = "iot"
};

/* ============================================================================
 * Random Number Generation
 * ============================================================================ */

static const char *HOSTS[] = {
    "server000", "server001", "server002", "server003", "server004",
    "server005", "server006", "server007", "server008", "server009",
    "server010", "server011", "server012", "server013", "server014",
    "server015", "server016", "server017", "server018", "server019",
    "server020", "server021", "server022", "server023", "server024",
    "server025", "server026", "server027", "server028", "server029",
    "server030", "server031", "server032", "server033", "server034",
    "server035", "server036", "server037", "server038", "server039",
    "server040", "server041", "server042", "server043", "server044",
    "server045", "server046", "server047", "server048", "server049"
};
#define NUM_HOSTS 50

static const char *SYMBOLS[] = {
    "AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
    "WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX"
};
#define NUM_SYMBOLS 20

static const char *EXCHANGES[] = {"NYSE", "NASDAQ", "ARCA", "BATS", "IEX"};
#define NUM_EXCHANGES 5

/* Fast random number generator (xorshift64) - thread local */
static __thread uint64_t rng_state = 0;

static inline uint64_t xorshift64(void) {
    if (rng_state == 0) {
        rng_state = (uint64_t)time(NULL) ^ (uint64_t)pthread_self() ^ (uint64_t)rand();
    }
    uint64_t x = rng_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    rng_state = x;
    return x;
}

static inline double rand_double(void) {
    return (double)(xorshift64() & 0x7FFFFFFF) / (double)0x7FFFFFFF;
}

static inline int rand_int(int max) {
    return (int)(xorshift64() % max);
}

/* Get current time in microseconds */
static inline int64_t now_micros(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000LL + (int64_t)ts.tv_nsec / 1000LL;
}

/* Get current time in nanoseconds (for latency measurement) */
static inline int64_t now_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

/* ============================================================================
 * MessagePack Columnar Format Generation
 *
 * Format matches Arc's expected structure:
 * {
 *   "m": "measurement_name",
 *   "columns": {
 *     "time": [int64, int64, ...],
 *     "host": ["str", "str", ...],
 *     "value": [float64, float64, ...],
 *     ...
 *   }
 * }
 * ============================================================================ */

static batch_t generate_iot_batch(int batch_size) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    /* Root map with 2 keys: "m" and "columns" */
    msgpack_pack_map(&pk, 2);

    /* "m": "cpu" */
    msgpack_pack_str(&pk, 1);
    msgpack_pack_str_body(&pk, "m", 1);
    msgpack_pack_str(&pk, 3);
    msgpack_pack_str_body(&pk, "cpu", 3);

    /* "columns": { ... } */
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "columns", 7);
    msgpack_pack_map(&pk, 5);  /* 5 columns: time, host, value, cpu_idle, cpu_user */

    int64_t base_ts = now_micros();

    /* Column: time (array of int64) */
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "time", 4);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_int64(&pk, base_ts + i);
    }

    /* Column: host (array of strings) */
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "host", 4);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        const char *host = HOSTS[rand_int(NUM_HOSTS)];
        size_t len = strlen(host);
        msgpack_pack_str(&pk, len);
        msgpack_pack_str_body(&pk, host, len);
    }

    /* Column: value (array of float64) */
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "value", 5);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_double(&pk, rand_double() * 100.0);
    }

    /* Column: cpu_idle (array of float64) */
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "cpu_idle", 8);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_double(&pk, rand_double() * 100.0);
    }

    /* Column: cpu_user (array of float64) */
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "cpu_user", 8);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_double(&pk, rand_double() * 100.0);
    }

    /* Copy buffer and clean up */
    batch_t batch;
    batch.data = malloc(sbuf.size);
    batch.size = sbuf.size;
    memcpy(batch.data, sbuf.data, sbuf.size);

    msgpack_sbuffer_destroy(&sbuf);

    return batch;
}

static batch_t generate_financial_batch(int batch_size) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    /* Root map with 2 keys: "m" and "columns" */
    msgpack_pack_map(&pk, 2);

    /* "m": "trades" */
    msgpack_pack_str(&pk, 1);
    msgpack_pack_str_body(&pk, "m", 1);
    msgpack_pack_str(&pk, 6);
    msgpack_pack_str_body(&pk, "trades", 6);

    /* "columns": { ... } */
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "columns", 7);
    msgpack_pack_map(&pk, 10);  /* 10 columns */

    int64_t base_ts = now_micros();

    /* Column: time */
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "time", 4);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_int64(&pk, base_ts + i);
    }

    /* Column: symbol */
    msgpack_pack_str(&pk, 6);
    msgpack_pack_str_body(&pk, "symbol", 6);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        const char *sym = SYMBOLS[rand_int(NUM_SYMBOLS)];
        size_t len = strlen(sym);
        msgpack_pack_str(&pk, len);
        msgpack_pack_str_body(&pk, sym, len);
    }

    /* Column: exchange */
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "exchange", 8);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        const char *ex = EXCHANGES[rand_int(NUM_EXCHANGES)];
        size_t len = strlen(ex);
        msgpack_pack_str(&pk, len);
        msgpack_pack_str_body(&pk, ex, len);
    }

    /* Column: price */
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "price", 5);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_double(&pk, 10.0 + rand_double() * 490.0);
    }

    /* Column: bid */
    msgpack_pack_str(&pk, 3);
    msgpack_pack_str_body(&pk, "bid", 3);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        double base = 10.0 + rand_double() * 490.0;
        msgpack_pack_double(&pk, base - rand_double() * 0.04 - 0.01);
    }

    /* Column: ask */
    msgpack_pack_str(&pk, 3);
    msgpack_pack_str_body(&pk, "ask", 3);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        double base = 10.0 + rand_double() * 490.0;
        msgpack_pack_double(&pk, base + rand_double() * 0.04 + 0.01);
    }

    /* Column: bid_size */
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "bid_size", 8);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_int32(&pk, 100 + rand_int(9900));
    }

    /* Column: ask_size */
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "ask_size", 8);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_int32(&pk, 100 + rand_int(9900));
    }

    /* Column: volume */
    msgpack_pack_str(&pk, 6);
    msgpack_pack_str_body(&pk, "volume", 6);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_int32(&pk, 1 + rand_int(999));
    }

    /* Column: trade_id */
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "trade_id", 8);
    msgpack_pack_array(&pk, batch_size);
    for (int i = 0; i < batch_size; i++) {
        msgpack_pack_int32(&pk, 1000000 + rand_int(8999999));
    }

    /* Copy buffer and clean up */
    batch_t batch;
    batch.data = malloc(sbuf.size);
    batch.size = sbuf.size;
    memcpy(batch.data, sbuf.data, sbuf.size);

    msgpack_sbuffer_destroy(&sbuf);

    return batch;
}

/* Generate all batches */
static batch_t *generate_batches(int count, int batch_size, const char *data_type) {
    batch_t *batches = malloc(count * sizeof(batch_t));
    if (!batches) {
        fprintf(stderr, "Failed to allocate batches array\n");
        exit(1);
    }

    printf("Pre-generating %d batches (%s data)...\n", count, data_type);

    bool is_financial = (strcmp(data_type, "financial") == 0);

    for (int i = 0; i < count; i++) {
        if (is_financial) {
            batches[i] = generate_financial_batch(batch_size);
        } else {
            batches[i] = generate_iot_batch(batch_size);
        }

        if ((i + 1) % 100 == 0) {
            printf("  Progress: %d/%d\n", i + 1, count);
        }
    }

    return batches;
}

/* ============================================================================
 * Stats Management
 * ============================================================================ */

static stats_t *stats_create(size_t latency_capacity) {
    stats_t *s = calloc(1, sizeof(stats_t));
    if (!s) return NULL;

    s->latencies = malloc(latency_capacity * sizeof(double));
    s->latency_capacity = latency_capacity;
    pthread_mutex_init(&s->latency_mutex, NULL);
    atomic_store(&s->running, true);

    return s;
}

static void stats_add_latency(stats_t *s, double ms) {
    pthread_mutex_lock(&s->latency_mutex);
    long idx = atomic_load(&s->latency_count);
    if ((size_t)idx < s->latency_capacity) {
        s->latencies[idx] = ms;
        atomic_fetch_add(&s->latency_count, 1);
    }
    pthread_mutex_unlock(&s->latency_mutex);
}

static int compare_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

static double stats_percentile(stats_t *s, double p) {
    pthread_mutex_lock(&s->latency_mutex);

    long count = atomic_load(&s->latency_count);
    if (count == 0) {
        pthread_mutex_unlock(&s->latency_mutex);
        return 0.0;
    }

    /* Copy and sort */
    double *sorted = malloc(count * sizeof(double));
    memcpy(sorted, s->latencies, count * sizeof(double));
    qsort(sorted, count, sizeof(double), compare_double);

    int idx = (int)(count * p);
    if (idx >= count) idx = count - 1;
    double result = sorted[idx];

    free(sorted);
    pthread_mutex_unlock(&s->latency_mutex);

    return result;
}

/* ============================================================================
 * HTTP Worker (using libcurl)
 * ============================================================================ */

/* Discard response body */
static size_t discard_callback(void *ptr, size_t size, size_t nmemb, void *data) {
    (void)ptr; (void)data;
    return size * nmemb;
}

static void *worker_thread(void *arg) {
    worker_ctx_t *ctx = (worker_ctx_t *)arg;
    stats_t *stats = ctx->stats;
    config_t *cfg = ctx->cfg;

    int batch_idx = ctx->id;  /* Start at different offsets */

    while (atomic_load(&stats->running)) {
        batch_t *batch = &ctx->batches[batch_idx % ctx->batch_count];
        batch_idx++;

        int64_t start = now_nanos();

        /* Set request body */
        curl_easy_setopt(ctx->curl, CURLOPT_POSTFIELDSIZE, (long)batch->size);
        curl_easy_setopt(ctx->curl, CURLOPT_POSTFIELDS, batch->data);

        CURLcode res = curl_easy_perform(ctx->curl);

        int64_t elapsed_ns = now_nanos() - start;
        double latency_ms = (double)elapsed_ns / 1000000.0;

        if (res == CURLE_OK) {
            long http_code;
            curl_easy_getinfo(ctx->curl, CURLINFO_RESPONSE_CODE, &http_code);

            if (http_code == 204) {
                atomic_fetch_add(&stats->total_sent, cfg->batch_size);
                stats_add_latency(stats, latency_ms);
            } else {
                atomic_fetch_add(&stats->total_errors, 1);
            }
        } else {
            atomic_fetch_add(&stats->total_errors, 1);
        }
    }

    return NULL;
}

/* ============================================================================
 * Main
 * ============================================================================ */

static void print_usage(const char *prog) {
    printf("Usage: %s [options]\n\n", prog);
    printf("Options:\n");
    printf("  -d, --duration <sec>     Test duration (default: 60)\n");
    printf("  -w, --workers <n>        Number of workers (default: 100)\n");
    printf("  -b, --batch-size <n>     Records per batch (default: 1000)\n");
    printf("  -p, --pregenerate <n>    Batches to pre-generate (default: 1000)\n");
    printf("  -t, --data-type <type>   Data type: iot, financial (default: iot)\n");
    printf("  -H, --host <host>        Server host (default: localhost)\n");
    printf("  -P, --port <port>        Server port (default: 8000)\n");
    printf("  -h, --help               Show this help\n");
}

int main(int argc, char *argv[]) {
    config_t cfg = default_config;
    cfg.token = getenv("ARC_TOKEN");

    /* Parse arguments */
    static struct option long_opts[] = {
        {"duration",    required_argument, 0, 'd'},
        {"workers",     required_argument, 0, 'w'},
        {"batch-size",  required_argument, 0, 'b'},
        {"pregenerate", required_argument, 0, 'p'},
        {"data-type",   required_argument, 0, 't'},
        {"host",        required_argument, 0, 'H'},
        {"port",        required_argument, 0, 'P'},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "d:w:b:p:t:H:P:h", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'd': cfg.duration = atoi(optarg); break;
            case 'w': cfg.workers = atoi(optarg); break;
            case 'b': cfg.batch_size = atoi(optarg); break;
            case 'p': cfg.pregenerate = atoi(optarg); break;
            case 't': cfg.data_type = optarg; break;
            case 'H': cfg.host = optarg; break;
            case 'P': cfg.port = atoi(optarg); break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    const char *data_label = (strcmp(cfg.data_type, "financial") == 0)
        ? "FINANCIAL (Stock Trades)" : "IOT (Server Metrics)";

    /* Print banner */
    printf("================================================================================\n");
    printf("SUSTAINED LOAD TEST - C CLIENT (libcurl + msgpack)\n");
    printf("================================================================================\n");
    printf("Target: http://%s:%d/api/v1/write/msgpack\n", cfg.host, cfg.port);
    printf("Protocol: MessagePack Columnar\n");
    printf("Data type: %s\n", data_label);
    printf("Duration: %ds\n", cfg.duration);
    printf("Batch size: %d\n", cfg.batch_size);
    printf("Workers: %d\n", cfg.workers);
    printf("Pre-generate: %d batches\n", cfg.pregenerate);
    printf("================================================================================\n\n");

    /* Initialize libcurl */
    curl_global_init(CURL_GLOBAL_ALL);

    /* Generate batches */
    int64_t gen_start = now_nanos();
    batch_t *batches = generate_batches(cfg.pregenerate, cfg.batch_size, cfg.data_type);
    double gen_time = (double)(now_nanos() - gen_start) / 1e9;

    size_t total_size = 0;
    for (int i = 0; i < cfg.pregenerate; i++) {
        total_size += batches[i].size;
    }
    double avg_size = (double)total_size / cfg.pregenerate;

    printf("Generated %d batches in %.1fs\n", cfg.pregenerate, gen_time);
    printf("  Avg size: %.1f KB (msgpack, uncompressed)\n\n", avg_size / 1024.0);

    if (cfg.token) {
        printf("Using auth token: %.8s...\n", cfg.token);
    } else {
        printf("No ARC_TOKEN set - authentication may fail\n");
    }

    /* Create stats */
    stats_t *stats = stats_create(1000000);  /* 1M latency samples */

    /* Build URL and headers */
    char url[256];
    snprintf(url, sizeof(url), "http://%s:%d/api/v1/write/msgpack", cfg.host, cfg.port);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/msgpack");
    headers = curl_slist_append(headers, "x-arc-database: production");

    if (cfg.token) {
        char auth_header[256];
        snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", cfg.token);
        headers = curl_slist_append(headers, auth_header);
    }

    /* Create workers */
    printf("\nStarting %d workers...\n", cfg.workers);

    pthread_t *threads = malloc(cfg.workers * sizeof(pthread_t));
    worker_ctx_t *contexts = malloc(cfg.workers * sizeof(worker_ctx_t));

    for (int i = 0; i < cfg.workers; i++) {
        contexts[i].id = i;
        contexts[i].cfg = &cfg;
        contexts[i].batches = batches;
        contexts[i].batch_count = cfg.pregenerate;
        contexts[i].stats = stats;
        contexts[i].headers = headers;
        strcpy(contexts[i].url, url);

        /* Create curl handle for this worker */
        contexts[i].curl = curl_easy_init();
        curl_easy_setopt(contexts[i].curl, CURLOPT_URL, url);
        curl_easy_setopt(contexts[i].curl, CURLOPT_POST, 1L);
        curl_easy_setopt(contexts[i].curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(contexts[i].curl, CURLOPT_WRITEFUNCTION, discard_callback);
        curl_easy_setopt(contexts[i].curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(contexts[i].curl, CURLOPT_TCP_NODELAY, 1L);
        curl_easy_setopt(contexts[i].curl, CURLOPT_TCP_KEEPALIVE, 1L);

        pthread_create(&threads[i], NULL, worker_thread, &contexts[i]);
    }

    /* Progress reporting */
    printf("Running for %d seconds...\n\n", cfg.duration);

    int64_t start_time = now_nanos();
    long last_sent = 0;

    for (int sec = 5; sec <= cfg.duration; sec += 5) {
        sleep(5);

        double elapsed = (double)(now_nanos() - start_time) / 1e9;
        long current_sent = atomic_load(&stats->total_sent);
        long interval_sent = current_sent - last_sent;
        double interval_rps = (double)interval_sent / 5.0;

        printf("[%6.1fs] RPS: %10.0f | Total: %12ld | Errors: %6ld\n",
               elapsed, interval_rps, current_sent, atomic_load(&stats->total_errors));

        last_sent = current_sent;
    }

    /* Stop workers */
    atomic_store(&stats->running, false);

    for (int i = 0; i < cfg.workers; i++) {
        pthread_join(threads[i], NULL);
        curl_easy_cleanup(contexts[i].curl);
    }

    /* Final stats */
    double elapsed = (double)(now_nanos() - start_time) / 1e9;
    long total_sent = atomic_load(&stats->total_sent);
    long total_errors = atomic_load(&stats->total_errors);
    double throughput = (double)total_sent / elapsed;

    printf("\n================================================================================\n");
    printf("RESULTS\n");
    printf("================================================================================\n");
    printf("Duration:        %.1fs\n", elapsed);
    printf("Total sent:      %ld records\n", total_sent);
    printf("Total errors:    %ld\n", total_errors);
    printf("Success rate:    %.2f%%\n", 100.0 * total_sent / (total_sent + (total_errors > 0 ? total_errors : 1)));
    printf("\n");
    printf("THROUGHPUT:   %.0f records/sec\n", throughput);
    printf("\n");
    printf("Latency percentiles:\n");
    printf("  p50:  %.2f ms\n", stats_percentile(stats, 0.50));
    printf("  p95:  %.2f ms\n", stats_percentile(stats, 0.95));
    printf("  p99:  %.2f ms\n", stats_percentile(stats, 0.99));
    printf("  p999: %.2f ms\n", stats_percentile(stats, 0.999));
    printf("================================================================================\n");

    /* Cleanup */
    curl_slist_free_all(headers);
    curl_global_cleanup();

    for (int i = 0; i < cfg.pregenerate; i++) {
        free(batches[i].data);
    }
    free(batches);
    free(threads);
    free(contexts);
    free(stats->latencies);
    free(stats);

    return 0;
}
