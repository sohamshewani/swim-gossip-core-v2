#define _GNU_SOURCE
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

#define MAX_NODES 64
#define MAX_METADATA 256
#define INITIAL_INCRASE_PERIOD_MS 10
#define TEST_DURATION_SEC 2

typedef enum {
    NODE_ALIVE = 0,
    NODE_SUSPECT = 1,
    NODE_DEAD = 2
} node_state_t;

typedef struct {
    uint32_t id;
    node_state_t state;
    uint64_t incarnation;
    char metadata[MAX_METADATA];
    uint64_t last_updated_ms;
} node_meta_t;

typedef struct {
    pthread_mutex_t lock;
    node_meta_t nodes[MAX_NODES];
    size_t node_count;
    uint32_t self_id;
    uint64_t self_incarnation;
    int running;
} swim_cluster_t;

static uint64_t get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec * 1000ULL) + (ts.tv_nsec / 1000000ULL);
}

static void swim_cluster_init(swim_cluster_t *c, uint32_t self_id) {
    memset(c, 0, sizeof(swim_cluster_t));
    pthread_mutex_init(&c->lock, NULL);
    c->self_id = self_id;
    c->self_incarnation = 1;
    c->running = 1;
}

static void swim_cluster_destroy(swim_cluster_t *c) {
    pthread_mutex_destroy(&c->lock);
}

static int swim_upsert_node(swim_cluster_t *c, uint32_t id, node_state_t state, uint64_t inc, const char *meta) {
    pthread_mutex_lock(&c->lock);
    
    size_t found = (size_t)-1;
    for (size_t i = 0; i < c->node_count; ++i) {
        if (c->nodes[i].id == id) {
            found = i;
            break;
        }
    }

    if (found == (size_t)-1) {
        if (c->node_count >= MAX_NODES) {
            pthread_mutex_unlock(&c->lock);
            return -1; 
        }
        found = c->node_count++;
        c->nodes[found].id = id;
        c->nodes[found].state = state;
        c->nodes[found].incarnation = inc;
        snprintf(c->nodes[found].metadata, MAX_METADATA, "%s", meta);
        c->nodes[found].last_updated_ms = get_time_ms();
        pthread_mutex_unlock(&c->lock);
        return 1; 
    }

    node_meta_t *n = &c->nodes[found];
    int updated = 0;

    if (inc > n->incarnation) {
        n->incarnation = inc;
        n->state = state;
        snprintf(n->metadata, MAX_METADATA, "%s", meta);
        updated = 1;
    } else if (inc == n->incarnation) {
        if (n->state == NODE_ALIVE && (state == NODE_SUSPECT || state == NODE_DEAD)) {
            n->state = state;
            updated = 1;
        } else if (n->state == NODE_SUSPECT && state == NODE_DEAD) {
            n->state = NODE_DEAD;
            updated = 1;
        }
        if (strncmp(n->metadata, meta, MAX_METADATA) != 0) {
            snprintf(n->metadata, MAX_METADATA, "%s", meta);
            updated = 1;
        }
    }

    if (updated) {
        n->last_updated_ms = get_time_ms();
    }

    pthread_mutex_unlock(&c->lock);
    return updated ? 2 : 0;
}

static node_state_t swim_get_node_state(swim_cluster_t *c, uint32_t id, uint64_t *inc_out) {
    pthread_mutex_lock(&c->lock);
    for (size_t i = 0; i < c->node_count; ++i) {
        if (c->nodes[i].id == id) {
            if (inc_out) *inc_out = c->nodes[i].incarnation;
            node_state_t s = c->nodes[i].state;
            pthread_mutex_unlock(&c->lock);
            return s;
        }
    }
    pthread_mutex_unlock(&c->lock);
    return NODE_DEAD;
}

static void test_unit_boundaries(void) {
    printf("[UNIT] Running boundary and edge-case assertions...\n");
    swim_cluster_t cluster;
    swim_cluster_init(&cluster, 1);

    assert(swim_upsert_node(&cluster, 100, NODE_ALIVE, 1, "meta1") == 1);
    assert(swim_upsert_node(&cluster, 100, NODE_ALIVE, 1, "meta1") == 0);

    assert(swim_upsert_node(&cluster, 100, NODE_SUSPECT, 1, "meta1") == 2);
    assert(swim_get_node_state(&cluster, 100, NULL) == NODE_SUSPECT);

    assert(swim_upsert_node(&cluster, 100, NODE_ALIVE, 1, "meta1") == 0);
    assert(swim_get_node_state(&cluster, 100, NULL) == NODE_SUSPECT);

    assert(swim_upsert_node(&cluster, 100, NODE_ALIVE, 2, "meta2") == 2);
    uint64_t inc = 0;
    assert(swim_get_node_state(&cluster, 100, &inc) == NODE_ALIVE);
    assert(inc == 2);

    assert(swim_upsert_node(&cluster, 100, NODE_DEAD, 2, "meta2") == 2);
    assert(swim_upsert_node(&cluster, 100, NODE_ALIVE, 2, "meta2") == 0);

    for (size_t i = 2; i < MAX_NODES + 5; ++i) {
        int res = swim_upsert_node(&cluster, (uint32_t)i, NODE_ALIVE, 1, "x");
        if (i < MAX_NODES) {
            assert(res == 1 || res == 2);
        } else {
            assert(res == -1);
        }
    }

    swim_cluster_destroy(&cluster);
    printf("[UNIT] Passed all boundary assertions.\n");
}

static void test_invariants(void) {
    printf("[INVARIANT] Verifying transition invariants and monotonicity...\n");
    swim_cluster_t cluster;
    swim_cluster_init(&cluster, 1);

    uint32_t target_id = 42;
    assert(swim_upsert_node(&cluster, target_id, NODE_ALIVE, 5, "v1") == 1);

    assert(swim_upsert_node(&cluster, target_id, NODE_ALIVE, 4, "v0") == 0);
    uint64_t inc = 0;
    assert(swim_get_node_state(&cluster, target_id, &inc) == NODE_ALIVE);
    assert(inc == 5);

    assert(swim_upsert_node(&cluster, target_id, NODE_SUSPECT, 5, "v1") == 2);
    assert(swim_upsert_node(&cluster, target_id, NODE_DEAD, 5, "v1") == 2);
    assert(swim_get_node_state(&cluster, target_id, &inc) == NODE_DEAD);

    assert(swim_upsert_node(&cluster, target_id, NODE_ALIVE, 5, "v1") == 0);
    assert(swim_get_node_state(&cluster, target_id, &inc) == NODE_DEAD);

    assert(swim_upsert_node(&cluster, target_id, NODE_ALIVE, 6, "v2") == 2);
    assert(swim_get_node_state(&cluster, target_id, &inc) == NODE_ALIVE);
    assert(inc == 6);

    swim_cluster_destroy(&cluster);
    printf("[INVARIANT] Passed all invariant checks.\n");
}

typedef struct {
    swim_cluster_t *cluster;
    int thread_id;
    int total_threads;
    uint64_t ops_performed;
} worker_arg_t;

static void *stress_worker(void *arg) {
    worker_arg_t *warg = (worker_arg_t *)arg;
    swim_cluster_t *c = warg->cluster;
    uint64_t count = 0;

    while (c->running) {
        uint32_t node_id = (uint32_t)((warg->thread_id + count) % (MAX_NODES - 1));
        uint64_t inc = (count % 10) + 1;
        node_state_t state = (node_state_t)(count % 3);
        char meta[32];
        snprintf(meta, sizeof(meta), "t%d-c%lu", warg->thread_id, count);

        swim_upsert_node(c, node_id, state, inc, meta);
        uint64_t out_inc;
        (void)swim_get_node_state(c, node_id, &out_inc);

        count++;
    }
    warg->ops_performed = count;
    return NULL;
}

static void test_concurrent_stress(void) {
    printf("[STRESS] Running multi-threaded contention and race detection (%d sec)...\n", TEST_DURATION_SEC);
    swim_cluster_t cluster;
    swim_cluster_init(&cluster, 1);

#define NUM_THREADS 8
    pthread_t threads[NUM_THREADS];
    worker_arg_t args[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; ++i) {
        args[i].cluster = &cluster;
        args[i].thread_id = i;
        args[i].total_threads = NUM_THREADS;
        args[i].ops_performed = 0;
        pthread_create(&threads[i], NULL, stress_worker, &args[i]);
    }

    sleep(TEST_DURATION_SEC);

    pthread_mutex_lock(&cluster.lock);
    cluster.running = 0;
    pthread_mutex_unlock(&cluster.lock);

    uint64_t total_ops = 0;
    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_join(threads[i], NULL);
        total_ops += args[i].ops_performed;
    }

    printf("[STRESS] Completed %lu concurrent operations across %d threads.\n", total_ops, NUM_THREADS);
    swim_cluster_destroy(&cluster);
}

static void test_failure_recovery(void) {
    printf("[RECOVERY] Testing node failure and self-heal incarnation escalation...\n");
    swim_cluster_t cluster;
    swim_cluster_init(&cluster, 10);

    uint32_t peer_id = 20;
    assert(swim_upsert_node(&cluster, peer_id, NODE_ALIVE, 1, "online") == 1);
    assert(swim_get_node_state(&cluster, peer_id, NULL) == NODE_ALIVE);

    assert(swim_upsert_node(&cluster, peer_id, NODE_SUSPECT,