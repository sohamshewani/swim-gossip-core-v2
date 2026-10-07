#ifndef SWIM_H
#define SWIM_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define SWIM_NODE_ID_LEN 16
#define SWIM_MAX_UDP_PAYLOAD 1280
#define SWIM_CACHE_LINE_SIZE 64
#define SWIM_DEFAULT_PORT 7946
#define SWIM_MPSC_QUEUE_CAPACITY 4096
#define SWIM_MAX_INDIRECT_PEERS 3
#define SWIM_HASH_MAP_INIT_CAPACITY 256
#define SWIM_VECTOR_CLOCK_MAX 64

typedef enum {
    SWIM_STATE_ALIVE = 0,
    SWIM_STATE_SUSPECTED = 1,
    SWIM_STATE_DEAD = 2,
    SWIM_STATE_LEFT = 3
} __attribute__((packed)) SwimNodeStateEnum;

typedef struct {
    uint8_t id[SWIM_NODE_ID_LEN];
} __attribute__((aligned(1))) SwimNodeID;

typedef struct {
    SwimNodeID node_id;
    uint64_t counter;
} __attribute__((aligned(8))) SwimVectorClockEntry;

typedef struct {
    SwimVectorClockEntry *entries;
    size_t count;
    size_t capacity;
} SwimVectorClock;

typedef struct {
    SwimNodeID node_id;
    struct sockaddr_in addr;
    _Atomic SwimNodeStateEnum state;
    _Atomic uint64_t incarnation;
    uint64_t suspicion_expiration_ms;
    SwimVectorClock vclock;
    uint32_t robin_hood_distance;
    bool occupied;
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) SwimNodeState;

typedef struct {
    SwimNodeState *buckets;
    size_t capacity;
    _Atomic size_t size;
    pthread_rwlock_t lock;
} SwimNodeRegistry;

typedef enum {
    SWIM_MSG_PING = 1,
    SWIM_MSG_ACK = 2,
    SWIM_MSG_PING_REQ = 3,
    SWIM_MSG_SUSPECT = 4,
    SWIM_MSG_ALIVE = 5,
    SWIM_MSG_DEAD = 6
} __attribute__((packed)) SwimMsgType;

typedef struct {
    uint8_t type;
    SwimNodeID sender_id;
    SwimNodeID target_id;
    uint64_t incarnation;
    uint8_t payload[SWIM_MAX_UDP_PAYLOAD - 33];
    size_t payload_len;
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) SwimMessage;

typedef struct SwimMPSCNode {
    SwimMessage msg;
    _Atomic(struct SwimMPSCNode*) next;
    uint32_t ttl;
} SwimMPSCNode;

typedef struct {
    _Atomic(SwimMPSCNode*) head;
    char pad1[SWIM_CACHE_LINE_SIZE - sizeof(_Atomic(SwimMPSCNode*))];
    SwimMPSCNode* tail;
    char pad2[SWIM_CACHE_LINE_SIZE - sizeof(SwimMPSCNode*)];
    _Atomic size_t size;
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) SwimMPSCQueue;

typedef struct {
    uint8_t *buffer;
    size_t capacity;
    size_t offset;
} SwimArenaBlock;

typedef struct {
    SwimArenaBlock *blocks;
    size_t block_count;
    size_t current_block;
    size_t block_size;
    pthread_mutex_t lock;
} SwimArenaAllocator;

typedef struct {
    int epoll_fd;
    int udp_socket;
    struct sockaddr_in bind_addr;
    _Atomic bool running;
    pthread_t reactor_thread;
    pthread_t ticker_thread;
    SwimNodeRegistry registry;
    SwimMPSCQueue piggyback_queue;
    SwimArenaAllocator arena;
    SwimNodeID self_id;
    uint64_t protocol_period_ms;
} SwimContext;

bool swim_node_id_equal(const SwimNodeID *a, const SwimNodeID *b);
uint64_t swim_hash_node_id(const SwimNodeID *id, size_t capacity);

bool swim_registry_init(SwimNodeRegistry *registry, size_t initial_capacity);
void swim_registry_destroy(SwimNodeRegistry *registry);
bool swim_registry_insert(SwimNodeRegistry *registry, const SwimNodeState *state);
SwimNodeState* swim_registry_get(SwimNodeRegistry *registry, const SwimNodeID *id);
bool swim_registry_remove(SwimNodeRegistry *registry, const SwimNodeID *id);

bool swim_mpsc_init(SwimMPSCQueue *queue);
void swim_mpsc_destroy(SwimMPSCQueue *queue);
bool swim_mpsc_push(SwimMPSCQueue *queue, SwimMessage *msg, uint32_t ttl);
bool swim_mpsc_pop(SwimMPSCQueue *queue, SwimMessage *msg, uint32_t *ttl);

bool swim_arena_init(SwimArenaAllocator *arena, size_t block_size, size_t initial_blocks);
void swim_arena_destroy(SwimArenaAllocator *arena);
void* swim_arena_alloc(SwimArenaAllocator *arena, size_t size);
void swim_arena_reset(SwimArenaAllocator *arena);

bool swim_context_init(SwimContext *ctx, const char *bind_ip, uint16_t bind_port, const SwimNodeID *self_id, uint64_t period_ms);
void swim_context_destroy(SwimContext *ctx);
bool swim_context_start(SwimContext *ctx);
void swim_context_stop(SwimContext *ctx);

bool swim_vclock_init(SwimVectorClock *vclock, size_t initial_capacity);
void swim_vclock_destroy(SwimVectorClock *vclock);
uint64_t swim_vclock_get(const SwimVectorClock *vclock, const SwimNodeID *node_id);
void swim_vclock_increment(SwimVectorClock *vclock, const SwimNodeID *node_id);
void swim_vclock_merge(SwimVectorClock *dest, const SwimVectorClock *src);

#endif

#ifndef SWIM_IMPL_H
#define SWIM_IMPL_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <time.h>
#include <arpa/inet.h>

inline bool swim_node_id_equal(const SwimNodeID *a, const SwimNodeID *b) {
    return memcmp(a->id, b->id, SWIM_NODE_ID_LEN) == 0;
}

uint64_t swim_hash_node_id(const SwimNodeID *id, size_t capacity) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < SWIM_NODE_ID_LEN; ++i) {
        hash ^= (uint64_t)id->id[i];
        hash *= 1099511628211ULL;
    }
    return hash % capacity;
}

bool swim_vclock_init(SwimVectorClock *vclock, size_t initial_capacity) {
    vclock->capacity = initial_capacity > 0 ? initial_capacity : 4;
    vclock->count = 0;
    vclock->entries = malloc(vclock->capacity * sizeof(SwimVectorClockEntry));
    return vclock->entries != NULL;
}

void swim_vclock_destroy(SwimVectorClock *vclock) {
    if (vclock->entries) {
        free(vclock->entries);
        vclock->entries = NULL;
    }
    vclock->count = 0;
    vclock->capacity = 0;
}

uint64_t swim_vclock_get(const SwimVectorClock *vclock, const SwimNodeID *node_id) {
    for (size_t i = 0; i < vclock->count; ++i) {
        if (swim_node_id_equal(&vclock->entries[i].node_id, node_id)) {
            return vclock->entries[i].counter;
        }
    }
    return 0;
}

void swim_vclock_increment(SwimVectorClock *vclock, const SwimNodeID *node_id) {
    for (size_t i = 0; i < vclock->count; ++i) {
        if (swim_node_id_equal(&vclock->entries[i].node_id, node_id)) {
            vclock->entries[i].counter++;
            return;
        }
    }
    if (vclock->count >= vclock->capacity) {
        size_t new_cap = vclock->capacity * 2;
        SwimVectorClockEntry *new_entries = realloc(vclock->entries, new_cap * sizeof(SwimVectorClockEntry));
        if (!new_entries) return;
        vclock->entries = new_entries;
        vclock->capacity = new_cap;
    }
    memcpy(&vclock->entries[vclock->count].node_id, node_id, sizeof(SwimNodeID));
    vclock->entries[vclock->count].counter = 1;
    vclock->count++;
}

void swim_vclock_merge(SwimVectorClock *dest, const SwimVectorClock *src) {
    for (size_t i = 0; i < src->count; ++i) {
        uint64_t src_val = src->entries[i].counter;
        uint64_t dest_val = swim_vclock_get(dest, &src->entries[i].node_id);
        if (src_val > dest_val) {
            bool found = false;
            for (size_t j = 0; j < dest->count; ++j) {
                if (swim_node_id_equal(&dest->entries[j].node_id, &src->entries[i].node_id)) {
                    dest->entries[j].counter = src_val;
                    found = true;
                    break;
                }
            }
            if (!found) {
                if (dest->count >= dest->capacity) {
                    size_t new_cap = dest->capacity * 2;
                    SwimVectorClockEntry *new_entries = realloc(dest->entries, new_cap * sizeof(SwimVectorClockEntry));
                    if (!new_entries) return;
                    dest->entries = new_entries;
                    dest->capacity = new_cap;
                }
                memcpy(&dest->entries[dest->count].node_id, &src->entries[i].node_id, sizeof(SwimNodeID));
                dest->entries[dest->count].counter = src_val;
                dest->count++;
            }
        }
    }
}

bool swim_registry_init(SwimNodeRegistry *registry, size_t initial_capacity) {
    registry->capacity = initial_capacity;
    atomic_store(&registry->size, 0);
    registry->buckets = calloc(initial_capacity, sizeof(SwimNodeState));
    if (!registry->buckets) return false;
    if (pthread_rwlock_init(&registry->lock, NULL) != 0) {
        free(registry->buckets);
        return false;
    }
    return true;
}

void swim_registry_destroy(SwimNodeRegistry *registry) {
    pthread_rwlock_wrlock(&registry->lock);
    for (size_t i = 0; i < registry->capacity; ++i) {
        if (registry->buckets[i].occupied) {
            swim_vclock_destroy(&registry->buckets[i].vclock);
        }
    }
    free(registry->buckets);
    registry->buckets = NULL;
    pthread_rwlock_unlock(&registry->lock);
    pthread_rwlock_destroy(&registry->lock);
}

static void swim_registry_resize(SwimNodeRegistry *registry, size_t new_capacity) {
    SwimNodeState *old_buckets = registry->buckets;
    size_t old_capacity = registry->capacity;

    SwimNodeState *new_buckets = calloc(new_capacity, sizeof(SwimNodeState));
    if (!new_buckets) return;

    registry->buckets = new_buckets;
    registry->capacity = new_capacity;
    atomic_store(&registry->size, 0);

    for (size_t i = 0; i < old_capacity; ++i) {
        if (old_buckets[i].occupied) {
            SwimNodeState *state = &old_buckets[i];
            state->robin_hood_distance = 0;
            state->occupied = true;

            uint64_t idx = swim_hash_node_id(&state->node_id, registry->capacity);
            uint32_t distance = 0;

            while (true) {
                if (!registry->buckets[idx].occupied) {
                    registry->buckets[idx] = *state;
                    registry->buckets[idx].robin_hood_distance = distance;
                    registry->buckets[idx].occupied = true;
                    atomic_fetch_add(&registry->size, 1);
                    break;
                }

                if (registry->buckets[idx].robin_hood_distance < distance) {
                    SwimNodeState temp = registry->buckets[idx];
                    registry->buckets[idx] = *state;
                    registry->buckets[idx].robin_hood_distance = distance;
                    registry->buckets[idx].occupied = true;
                    *state = temp;
                    distance = state->robin_hood_distance;
                }

                idx = (idx + 1) % registry->capacity;
                distance++;
            }
        }
    }
    free(old_buckets);
}

bool swim_registry_insert(SwimNodeRegistry *registry, const SwimNodeState *state) {
    pthread_rwlock_wrlock(&registry->lock);
    if ((atomic_load(&registry->size) + 1) * 100 / registry->capacity > 75) {
        swim_registry_resize(registry, registry->capacity * 2);
    }

    SwimNodeState to_insert = *state;
    to_insert.robin_hood_distance = 0;
    to_insert.occupied = true;

    uint64_t idx = swim_hash_node_id(&to_insert.node_id, registry->capacity);
    uint32_t distance = 0;

    while (true) {
        if (!registry->buckets[idx].occupied) {
            registry->buckets[idx] = to_insert;
            registry->buckets[idx].robin_hood_distance = distance;
            registry->buckets[idx].occupied = true;
            atomic_fetch_add(&registry->size, 1);
            pthread_rwlock_unlock(&registry->lock);
            return true;
        }

        if (swim_node_id_equal(&registry->buckets[idx].node_id, &to_insert.node_id)) {
            registry->buckets[idx].state = state->state;
            atomic_store(&registry->buckets[idx].incarnation, atomic_load(&state->incarnation));
            registry->buckets[idx].suspicion_expiration_ms = state->suspicion_expiration_ms;
            registry->buckets[idx].addr = state->addr;
            swim_vclock_merge(&registry->buckets[idx].vclock, &state->vclock);
            pthread_rwlock_unlock(&registry->lock);
            return true;
        }

        if (registry->buckets[idx].robin_hood_distance < distance) {
            SwimNodeState temp = registry->buckets[idx];
            registry->buckets[idx] = to_insert;
            registry->buckets[idx].robin_hood_distance = distance;
            registry->buckets[idx].occupied = true;
            to_insert = temp;
            distance = to_insert.robin_hood_distance;
        }

        idx = (idx + 1) % registry->capacity;
        distance++;
    }
}

SwimNodeState* swim_registry_get(SwimNodeRegistry *registry, const SwimNode