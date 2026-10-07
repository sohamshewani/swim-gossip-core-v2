#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>

#define CACHE_LINE_SIZE 64
#define MAX_UDP_PAYLOAD 1280
#define UUID_STR_LEN 37
#define INITIAL_BUCKET_COUNT 1024
#define MPSC_RING_CAPACITY 4096
#define MAX_PIGGYBACK_ITEMS 256
#define MAX_INDIRECT_PROBES 3
#define PROTOCOL_PERIOD_MS 1000

typedef uint8_t node_id_t[16];

typedef enum __attribute__((__packed__)) {
    NODE_STATE_ALIVE = 0,
    NODE_STATE_SUSPECTED = 1,
    NODE_STATE_DEAD = 2,
    NODE_STATE_LEFT = 3
} node_state_enum_t;

typedef struct {
    node_id_t node_id;
    uint64_t counter;
} __attribute__((aligned(8))) vector_clock_tuple_t;

typedef struct {
    vector_clock_tuple_t *tuples;
    size_t count;
    size_t capacity;
} vector_clock_t;

typedef struct {
    node_id_t node_id;
    char ip_str[INET6_ADDRSTRLEN];
    uint16_t port;
    node_state_enum_t state;
    uint32_t incarnation;
    uint64_t suspicion_expiry_ms;
    vector_clock_t vclock;
    atomic_uint_fast32_t reference_count;
} __attribute__((aligned(CACHE_LINE_SIZE))) NodeState;

typedef struct {
    uint32_t hash_val;
    int32_t probe_length;
    atomic_bool occupied;
    NodeState *node;
} __attribute__((aligned(CACHE_LINE_SIZE))) bucket_t;

typedef struct {
    bucket_t *buckets;
    size_t capacity;
    atomic_size_t size;
    pthread_rwlock_t lock;
} node_registry_t;

typedef struct {
    uint8_t payload[MAX_UDP_PAYLOAD];
    size_t length;
    uint32_t ttl;
} piggyback_item_t;

typedef struct {
    piggyback_item_t items[MPSC_RING_CAPACITY];
    atomic_size_t head;
    atomic_size_t tail;
} mpsc_ring_t;

typedef struct {
    int epoll_fd;
    int udp_socket;
    atomic_bool running;
    pthread_t reactor_thread;
    pthread_t ticker_thread;
    mpsc_ring_t piggyback_queue;
    node_registry_t registry;
    node_id_t self_id;
    uint16_t self_port;
    char self_ip[INET6_ADDRSTRLEN];
} swim_engine_t;

static uint32_t murmurhash3_32(const void *key, size_t len, uint32_t seed) {
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 4;
    uint32_t h1 = seed;
    uint32_t c1 = 0xcc9e2d51;
    uint32_t c2 = 0x1b873593;
    const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;
        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }
    const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >> 17);
            k1 *= c2;
            h1 ^= k1;
    }
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    return h1;
}

static uint64_t get_current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

static void vector_clock_init(vector_clock_t *vc, size_t initial_capacity) {
    vc->capacity = initial_capacity > 0 ? initial_capacity : 4;
    vc->count = 0;
    vc->tuples = malloc(vc->capacity * sizeof(vector_clock_tuple_t));
}

static void vector_clock_free(vector_clock_t *vc) {
    if (vc->tuples) {
        free(vc->tuples);
        vc->tuples = NULL;
    }
    vc->count = 0;
    vc->capacity = 0;
}

static uint64_t vector_clock_get(const vector_clock_t *vc, const node_id_t node_id) {
    for (size_t i = 0; i < vc->count; i++) {
        if (memcmp(vc->tuples[i].node_id, node_id, 16) == 0) {
            return vc->tuples[i].counter;
        }
    }
    return 0;
}

static void vector_clock_set(vector_clock_t *vc, const node_id_t node_id, uint64_t counter) {
    for (size_t i = 0; i < vc->count; i++) {
        if (memcmp(vc->tuples[i].node_id, node_id, 16) == 0) {
            vc->tuples[i].counter = counter;
            return;
        }
    }
    if (vc->count >= vc->capacity) {
        vc->capacity *= 2;
        vc->tuples = realloc(vc->tuples, vc->capacity * sizeof(vector_clock_tuple_t));
    }
    memcpy(vc->tuples[vc->count].node_id, node_id, 16);
    vc->tuples[vc->count].counter = counter;
    vc->count++;
}

static void node_registry_init(node_registry_t *registry, size_t capacity) {
    registry->capacity = capacity;
    atomic_store(&registry->size, 0);
    registry->buckets = calloc(capacity, sizeof(bucket_t));
    pthread_rwlock_init(&registry->lock, NULL);
}

static void node_registry_free(node_registry_t *registry) {
    pthread_rwlock_wrlock(&registry->lock);
    for (size_t i = 0; i < registry->capacity; i++) {
        if (atomic_load(&registry->buckets[i].occupied)) {
            if (registry->buckets[i].node) {
                vector_clock_free(&registry->buckets[i].node->vclock);
                free(registry->buckets[i].node);
            }
        }
    }
    free(registry->buckets);
    pthread_rwlock_unlock(&registry->lock);
    pthread_rwlock_destroy(&registry->lock);
}

static NodeState *node_state_create(const node_id_t node_id, const char *ip, uint16_t port) {
    NodeState *ns = malloc(sizeof(NodeState));
    memcpy(ns->node_id, node_id, 16);
    strncpy(ns->ip_str, ip, INET6_ADDRSTRLEN - 1);
    ns->ip_str[INET6_ADDRSTRLEN - 1] = '\0';
    ns->port = port;
    ns->state = NODE_STATE_ALIVE;
    ns->incarnation = 0;
    ns->suspension_expiry_ms = 0;
    vector_clock_init(&ns->vclock, 4);
    atomic_init(&ns->reference_count, 1);
    return ns;
}

static bool node_registry_put(node_registry_t *registry, NodeState *node) {
    uint32_t hash = murmurhash3_32(node->node_id, 16, 0xdeadbeef);
    size_t idx = hash % registry->capacity;
    int32_t probe = 0;

    pthread_rwlock_wrlock(&registry->lock);

    if (atomic_load(&registry->size) >= registry->capacity * 0.75) {
        size_t old_cap = registry->capacity;
        size_t new_cap = old_cap * 2;
        bucket_t *old_buckets = registry->buckets;
        bucket_t *new_buckets = calloc(new_cap, sizeof(bucket_t));

        registry->capacity = new_cap;
        registry->buckets = new_buckets;
        atomic_store(&registry->size, 0);

        for (size_t i = 0; i < old_cap; i++) {
            if (atomic_load(&old_buckets[i].occupied)) {
                NodeState *n = old_buckets[i].node;
                uint32_t h = old_buckets[i].hash_val;
                size_t n_idx = h % new_cap;
                int32_t n_probe = 0;
                while (atomic_load(&registry->buckets[n_idx].occupied)) {
                    if (registry->buckets[n_idx].probe_length < n_probe) {
                        uint32_t th = registry->buckets[n_idx].hash_val;
                        int32_t tp = registry->buckets[n_idx].probe_length;
                        NodeState *tn = registry->buckets[n_idx].node;

                        registry->buckets[n_idx].hash_val = h;
                        registry->buckets[n_idx].probe_length = n_probe;
                        registry->buckets[n_idx].node = n;

                        h = th;
                        n_probe = tp;
                        n = tn;
                    }
                    n_idx = (n_idx + 1) % new_cap;
                    n_probe++;
                }
                registry->buckets[n_idx].hash_val = h;
                registry->buckets[n_idx].probe_length = n_probe;
                registry->buckets[n_idx].node = n;
                atomic_store(&registry->buckets[n_idx].occupied, true);
                atomic_fetch_add(&registry->size, 1);
            }
        }
        free(old_buckets);
        idx = hash % registry->capacity;
    }

    bucket_t local_bucket = {
        .hash_val = hash,
        .probe_length = 0,
        .node = node
    };

    while (atomic_load(&registry->buckets[idx].occupied)) {
        if (memcmp(registry->buckets[idx].node->node_id, node->node_id, 16) == 0) {
            vector_clock_free(&registry->buckets[idx].node->vclock);
            free(registry->buckets[idx].node);
            registry->buckets[idx].node = node;
            pthread_rwlock_unlock(&registry->lock);
            return true;
        }
        if (registry->buckets[idx].probe_length < probe) {
            uint32_t th = registry->buckets[idx].hash_val;
            int32_t tp = registry->buckets[idx].probe_length;
            NodeState *tn = registry->buckets[idx].node;

            registry->buckets[idx].hash_val = local_bucket.hash_val;
            registry->buckets[idx].probe_length = probe;
            registry->buckets[idx].node = local_bucket.node;

            local_bucket.hash_val = th;
            local_bucket.probe_length = tp;
            local_bucket.node = tn;
        }
        idx = (idx + 1) % registry->capacity;
        probe++;
        local_bucket.probe_length = probe;
    }

    registry->buckets[idx].hash_val = local_bucket.hash_val;
    registry->buckets[idx].probe_length = local_bucket.probe_length;
    registry->buckets[idx].node = local_bucket.node;
    atomic_store(&registry->buckets[idx].occupied, true);
    atomic_fetch_add(&registry->size, 1);

    pthread_rwlock_unlock(&registry->lock);
    return true;
}

static NodeState *node_registry_get(node_registry_t *registry, const node_id_t node_id) {
    uint32_t hash = murmurhash3_32(node_id, 16, 0xdeadbeef);
    pthread_rwlock_rdlock(&registry->lock);
    size_t idx = hash % registry->capacity;
    int32_t probe = 0;

    while (atomic_load(&registry->buckets[idx].occupied)) {
        if (registry->buckets[idx].probe_length < probe) {
            break;
        }
        if (memcmp(registry->buckets[idx].node->node_id, node_id, 16) == 0) {
            NodeState *node = registry->buckets[idx].node;
            atomic_fetch_add(&node->reference_count, 1);
            pthread_rwlock_unlock(&registry->lock);
            return node;
        }
        idx = (idx + 1) % registry->capacity;
        probe++;
    }
    pthread_rwlock_unlock(&registry->lock);
    return NULL;
}

static void node_state_release(NodeState *node) {
    if (node && atomic_fetch_sub(&node->reference_count, 1) == 1) {
        vector_clock_free(&node->vclock);
        free(node);
    }
}

static bool mpsc_ring_push(mpsc_ring_t *ring, const uint8_t *payload, size_t len, uint32_t ttl) {
    if (len > MAX_UDP_PAYLOAD) return false;
    size_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    size_t next_tail;
    do {
        next_tail = (tail + 1) % MPSC_RING_CAPACITY;
        if (next_tail == atomic_load_explicit(&ring->head, memory_order_acquire)) {
            return false;
        }
    } while (!atomic_compare_exchange_weak_explicit(&ring->tail, &tail, next_tail,
                                                    memory_order_release, memory_order_relaxed));
    memcpy(ring->items[tail].payload, payload, len);
    ring->items[tail].length = len;
    ring->items[tail].ttl = ttl;
    return true;
}

static bool mpsc_ring_pop(mpsc_ring_t *ring, piggyback_item_t *item) {
    size_t head = atomic_load_explicit(&ring->