#ifndef SWIM_TYPES_H
#define SWIM_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>

#define SWIM_CACHE_LINE_SIZE 64
#define SWIM_UUID_BYTES 16
#define SWIM_MAX_UDP_PAYLOAD 1280
#define SWIM_DEFAULT_PORT 7946
#define SWIM_MAX_INDIRECT_TARGETS 3

typedef uint8_t swim_uuid_t[SWIM_UUID_BYTES];

typedef enum {
    SWIM_STATE_ALIVE = 0,
    SWIM_STATE_SUSPECTED = 1,
    SWIM_STATE_DEAD = 2,
    SWIM_STATE_LEFT = 3,
    SWIM_STATE_JOINING = 4
} __attribute__((packed)) swim_node_state_enum_t;

typedef enum {
    SWIM_MSG_PING = 1,
    SWIM_MSG_ACK = 2,
    SWIM_MSG_PING_REQ = 3,
    SWIM_MSG_SWIM_COMPOSITE = 4
} __attribute__((packed)) swim_msg_type_t;

typedef struct {
    swim_uuid_t node_id;
    uint64_t counter;
} __attribute__((aligned(8))) swim_vector_clock_element_t;

typedef struct {
    swim_vector_clock_element_t *elements;
    uint32_t count;
    uint32_t capacity;
} swim_vector_clock_t;

typedef struct {
    swim_uuid_t node_id;
    char ip_address[40];
    uint16_t port;
    uint16_t padding;
    
    atomic_int state; // swim_node_state_enum_t
    atomic_uint_least64_t incarnation;
    
    uint64_t suspicion_expiration_ms;
    uint64_t last_heartbeat_ms;
    
    swim_vector_clock_t vclock;
    
    uint32_t robin_hood_dib;
    uint32_t flags;
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) swim_node_state_t;

typedef struct {
    swim_node_state_t *buckets;
    uint32_t capacity;
    atomic_uint_least32_t size;
    uint32_t mask;
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) swim_node_registry_t;

typedef struct {
    uint8_t *data;
    uint32_t length;
    uint32_t ttl;
    uint64_t timestamp_ms;
} swim_piggyback_item_t;

typedef struct {
    swim_piggyback_item_t *buffer;
    uint32_t capacity;
    uint32_t mask;
    atomic_uint_least32_t head;
    atomic_uint_least32_t tail;
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) swim_mpsc_ring_t;

typedef struct {
    uint8_t *base_address;
    size_t capacity;
    atomic_size_t offset;
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) swim_arena_t;

typedef struct {
    swim_msg_type_t type;
    swim_uuid_t sender_id;
    swim_uuid_t target_id;
    uint64_t incarnation;
    uint32_t payload_len;
    uint8_t payload[SWIM_MAX_UDP_PAYLOAD - 44];
} __attribute__((aligned(SWIM_CACHE_LINE_SIZE))) swim_packet_t;

static inline uint64_t swim_current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

static inline bool swim_uuid_equal(const swim_uuid_t a, const swim_uuid_t b) {
    const uint64_t *a64 = (const uint64_t *)a;
    const uint64_t *b64 = (const uint64_t *)b;
    return (a64[0] == b64[0]) && (a64[1] == b64[1]);
}

static inline void swim_uuid_copy(swim_uuid_t dest, const swim_uuid_t src) {
    uint64_t *d64 = (uint64_t *)dest;
    const uint64_t *s64 = (const uint64_t const *)src;
    d64[0] = s64[0];
    d64[1] = s64[1];
}

static inline uint64_t swim_hash_uuid(const swim_uuid_t uuid) {
    const uint64_t *data = (const uint64_t *)uuid;
    uint64_t h = data[0] ^ data[1];
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    return h;
}

static inline bool swim_arena_init(swim_arena_t *arena, size_t capacity) {
    if (!arena || capacity == 0) return false;
    arena->base_address = (uint8_t *)aligned_alloc(SWIM_CACHE_LINE_SIZE, (capacity + SWIM_CACHE_LINE_SIZE - 1) & ~(SWIM_CACHE_LINE_SIZE - 1));
    if (!arena->base_address) return false;
    arena->capacity = capacity;
    atomic_init(&arena->offset, 0);
    return true;
}

static inline void *swim_arena_alloc(swim_arena_t *arena, size_t size) {
    if (!arena || size == 0) return NULL;
    size_t aligned_size = (size + 7) & ~7;
    size_t old_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    
    while (1) {
        if (old_offset + aligned_size > arena->capacity) {
            return NULL;
        }
        if (atomic_compare_exchange_weak_explicit(&arena->offset, &old_offset, old_offset + aligned_size,
                                                  memory_order_relaxed, memory_order_relaxed)) {
            return (void *)(arena->base_address + old_offset);
        }
    }
}

static inline void swim_arena_reset(swim_arena_t *arena) {
    if (arena) {
        atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    }
}

static inline void swim_arena_destroy(swim_arena_t *arena) {
    if (arena && arena->base_address) {
        free(arena->base_address);
        arena->base_address = NULL;
        arena->capacity = 0;
        atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    }
}

static inline bool swim_mpsc_init(swim_mpsc_ring_t *ring, uint32_t capacity_pow2) {
    if (!ring || (capacity_pow2 & (capacity_pow2 - 1)) != 0) return false;
    ring->buffer = (swim_piggyback_item_t *)aligned_alloc(SWIM_CACHE_LINE_SIZE, sizeof(swim_piggyback_item_t) * capacity_pow2);
    if (!ring->buffer) return false;
    ring->capacity = capacity_pow2;
    ring->mask = capacity_pow2 - 1;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    for (uint32_t i = 0; i < capacity_pow2; i++) {
        ring->buffer[i].data = NULL;
        ring->buffer[i].length = 0;
        ring->buffer[i].ttl = 0;
        ring->buffer[i].timestamp_ms = 0;
    }
    return true;
}

static inline bool swim_mpsc_push(swim_mpsc_ring_t *ring, const swim_piggyback_item_t *item) {
    uint32_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint32_t head;
    
    do {
        head = atomic_load_explicit(&ring->head, memory_order_acquire);
        if ((tail - head) >= ring->capacity) {
            return false;
        }
    } while (!atomic_compare_exchange_weak_explicit(&ring->tail, &tail, tail + 1,
                                                    memory_order_release, memory_order_relaxed));
    
    uint32_t index = tail & ring->mask;
    ring->buffer[index] = *item;
    
    while (atomic_load_explicit(&ring->head, memory_order_relaxed) != tail) {
        atomic_thread_fence(memory_order_release);
    }
    atomic_store_explicit(&ring->head, tail + 1, memory_order_release);
    return true;
}

static inline bool swim_mpsc_pop(swim_mpsc_ring_t *ring, swim_piggyback_item_t *item) {
    uint32_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint32_t tail;
    
    do {
        tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
        if (head == tail) {
            return false;
        }
        *item = ring->buffer[head & ring->mask];
    } while (!atomic_compare_exchange_weak_explicit(&ring->head, &head, head + 1,
                                                    memory_order_release, memory_order_relaxed));
    return true;
}

static inline void swim_mpsc_destroy(swim_mpsc_ring_t *ring) {
    if (ring && ring->buffer) {
        free(ring->buffer);
        ring->buffer = NULL;
        ring->capacity = 0;
        ring->mask = 0;
    }
}

static inline bool swim_registry_init(swim_node_registry_t *registry, uint32_t capacity_pow2) {
    if (!registry || (capacity_pow2 & (capacity_pow2 - 1)) != 0) return false;
    registry->buckets = (swim_node_state_t *)aligned_alloc(SWIM_CACHE_LINE_SIZE, sizeof(swim_node_state_t) * capacity_pow2);
    if (!registry->buckets) return false;
    registry->capacity = capacity_pow2;
    atomic_init(&registry->size, 0);
    registry->mask = capacity_pow2 - 1;
    
    for (uint32_t i = 0; i < capacity_pow2; i++) {
        memset(&registry->buckets[i], 0, sizeof(swim_node_state_t));
        atomic_init(&registry->buckets[i].state, SWIM_STATE_DEAD);
        atomic_init(&registry->buckets[i].incarnation, 0);
        registry->buckets[i].robin_hood_dib = 0;
    }
    return true;
}

static inline void swim_registry_destroy(swim_node_registry_t *registry) {
    if (registry && registry->buckets) {
        for (uint32_t i = 0; i < registry->capacity; i++) {
            if (registry->buckets[i].vclock.elements) {
                free(registry->buckets[i].vclock.elements);
            }
        }
        free(registry->buckets);
        registry->buckets = NULL;
        registry->capacity = 0;
        atomic_store_explicit(&registry->size, 0, memory_order_relaxed);
    }
}

static inline bool swim_registry_insert(swim_node_registry_t *registry, const swim_node_state_t *node) {
    uint64_t hash = swim_hash_uuid(node->node_id);
    uint32_t idx = (uint32_t)(hash & registry->mask);
    uint32_t dib = 0;
    
    swim_node_state_t current_node = *node;
    current_node.robin_hood_dib = 0;
    
    uint32_t current_size = atomic_load_explicit(&registry->size, memory_order_relaxed);
    if (current_size >= registry->capacity * 3 / 4) {
        return false;
    }

    while (true) {
        swim_node_state_t *slot = &registry->buckets[idx];
        swim_node_state_enum_t slot_state = (swim_node_state_enum_t)atomic_load_explicit(&slot->state, memory_order_relaxed);
        
        if (slot_state == SWIM_STATE_DEAD || slot_state == SWIM_STATE_LEFT) {
            *slot = current_node;
            atomic_store_explicit(&slot->state, atomic_load_explicit(&node->state, memory_order_relaxed), memory_order_release);
            atomic_store_explicit(&registry->size, current_size + 1, memory_order_relaxed);
            return true;
        }
        
        if (swim_uuid_equal(slot->node_id, current_node.node_id)) {
            *slot = current_node;
            atomic_store_explicit(&slot->state, atomic_load_explicit(&node->state, memory_order_relaxed), memory_order_release);
            return true;
        }
        
        uint64_t slot_hash = swim_hash_uuid(slot->node_id);
        uint32_t slot_home_idx = (uint32_t)(slot_hash & registry->mask);
        uint32_t slot_dib = (idx >= slot_home_idx) ? (idx - slot_home_idx) : (registry->capacity - slot_home_idx + idx);
        
        if (slot_dib < dib) {
            swim_node_state_t temp = *slot;
            *slot = current_node;
            current_node = temp;
            dib = slot_dib;
        }
        
        idx = (idx + 1) & registry->mask;
        dib++;
        current_node.robin_hood_dib = dib;
        
        if (dib > registry->capacity) {
            return false;
        }
    }
}

static inline swim_node_state_t *swim_registry_lookup(swim_node_registry_t *registry, const swim_uuid_t node_id) {
    uint64_t hash = swim_hash_uuid(node_id);
    uint32_t idx = (uint32_t)(hash & registry->mask);
    uint32_t dib = 0;
    
    while (true) {
        swim_node_state_t *slot = &registry->buckets[idx];
        swim_node_state_enum_t slot_state = (swim_node_state_enum_t)atomic_load_explicit(&slot->state, memory_order_relaxed);
        
        if (slot_state == SWIM_STATE_DEAD && slot->robin_hood_dib == 0 && slot->vclock.count == 0) {
            return NULL;
        }
        
        if (slot_state != SWIM_STATE_DEAD && swim_uuid_equal(slot->node_id, node_id)) {
            return slot;
        }
        
        uint64_t slot_hash = swim_hash_uuid(slot->node_id);
        uint32_t slot_home_idx = (uint32_t)(slot_hash & registry->mask);
        uint32_t slot_dib = (idx >= slot_home_idx) ? (idx - slot_home_idx) : (registry->capacity - slot_home_idx + idx);
        
        if