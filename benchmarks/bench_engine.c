#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

#define WARMUP_ITERATIONS 10000
#define BENCHMARK_ITERATIONS 1000000

// Mocking the core structures of swim-gossip-core for standalone compilation and microbenchmarking.
typedef enum {
    NODE_ALIVE = 0,
    NODE_SUSPECT = 1,
    NODE_DEAD = 2
} swim_node_state_t;

typedef struct {
    uint8_t id[16];
    uint32_t incarnation;
    swim_node_state_t state;
} swim_member_t;

typedef struct {
    swim_member_t *members;
    size_t capacity;
    size_t count;
    uint32_t local_incarnation;
} swim_gossip_context_t;

// Simulated core functions matching swim-gossip-core interface
static swim_gossip_context_t* swim_gossip_init(size_t capacity) {
    swim_gossip_context_t *ctx = (swim_gossip_context_t *)malloc(sizeof(swim_gossip_context_t));
    if (!ctx) return NULL;
    ctx->members = (swim_member_t *)malloc(sizeof(swim_member_t) * capacity);
    ctx->capacity = capacity;
    ctx->count = 0;
    ctx->local_incarnation = 1;
    return ctx;
}

static void swim_gossip_free(swim_gossip_context_t *ctx) {
    if (ctx) {
        free(ctx->members);
        free(ctx);
    }
}

// Target micro-operation: State update / Piggyback merge simulation
static inline void swim_gossip_update_member(swim_gossip_context_t *ctx, const uint8_t *id, uint32_t incarnation, swim_node_state_t state) {
    // Linear scan simulation or hash lookup representation for membership state merge
    for (size_t i = 0; i < ctx->count; i++) {
        if (memcmp(ctx->members[i].id, id, 16) == 0) {
            if (incarnation > ctx->members[i].incarnation) {
                ctx->members[i].incarnation = incarnation;
                ctx->members[i].state = state;
            }
            return;
        }
    }
    // If not found, insert if space permits
    if (ctx->count < ctx->capacity) {
        memcpy(ctx->members[ctx->count].id, id, 16);
        ctx->members[ctx->count].incarnation = incarnation;
        ctx->members[ctx->count].state = state;
        ctx->count++;
    }
}

static inline uint64_t get_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    if (fa < fb) return -1;
    if (fa > fb) return 1;
    return 0;
}

int main(void) {
    swim_gossip_context_t *ctx = swim_gossip_init(4096);
    if (!ctx) {
        fprintf(stderr, "Failed to initialize SWIM context\n");
        return 1;
    }

    // Populate initial state
    uint8_t sample_id[16];
    memset(sample_id, 0xAB, 16);
    swim_gossip_update_member(ctx, sample_id, 1, NODE_ALIVE);

    uint64_t *latencies = (uint64_t *)malloc(sizeof(uint64_t) * BENCHMARK_ITERATIONS);
    if (!latencies) {
        fprintf(stderr, "Failed to allocate latency tracking buffer\n");
        swim_gossip_free(ctx);
        return 1;
    }

    // Warmup phase
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        sample_id[0] = (uint8_t)(i & 0xFF);
        swim_gossip_update_member(ctx, sample_id, (uint32_t)(i + 2), NODE_ALIVE);
    }

    // Benchmark phase
    uint64_t start_time = get_nanos();

    for (size_t i = 0; i < BENCHMARK_ITERATIONS; i++) {
        sample_id[0] = (uint8_t)(i & 0xFF);
        uint64_t op_start = get_nanos();
        
        swim_gossip_update_member(ctx, sample_id, (uint32_t)(i + 100), NODE_SUSPECT);
        
        uint64_t op_end = get_nanos();
        latencies[i] = op_end - op_start;
    }

    uint64_t end_time = get_nanos();
    uint64_t total_duration_ns = end_time - start_time;

    // Compute metrics
    qsort(latencies, BENCHMARK_ITERATIONS, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(BENCHMARK_ITERATIONS * 0.50)];
    uint64_t p90 = latencies[(size_t)(BENCHMARK_ITERATIONS * 0.90)];
    uint64_t p99 = latencies[(size_t)(BENCHMARK_ITERATIONS * 0.99)];
    uint64_t p999 = latencies[(size_t)(BENCHMARK_ITERATIONS * 0.999)];

    double seconds = (double)total_duration_ns / 1000000000.0;
    double ops_sec = (double)BENCHMARK_ITERATIONS / seconds;

    // Output formatted tabular metrics to stdout
    printf("+------------------------------------------------------------+\n");
    printf("|          swim-gossip-core Microbenchmark Results           |\n");
    printf("+------------------------+-----------------------------------+\n");
    printf("| Metric                 | Value                             |\n");
    printf("+------------------------+-----------------------------------+\n");
    printf("| Total Iterations       | %-33u |\n", (unsigned int)BENCHMARK_ITERATIONS);
    printf("| Total Duration         | %-27.4f s |\n", seconds);
    printf("| Throughput             | %-25.2f ops/sec |\n", ops_sec);
    printf("| Latency p50            | %-27.2f ns |\n", (double)p50);
    printf("| Latency p90            | %-27.2f ns |\n", (double)p90);
    printf("| Latency p99            | %-27.2f ns |\n", (double)p99);
    printf("| Latency p99.9          | %-27.2f ns |\n", (double)p999);
    printf("+------------------------+-----------------------------------+\n");

    free(latencies);
    swim_gossip_free(ctx);
    return 0;
}