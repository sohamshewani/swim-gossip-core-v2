#include <stdatomic.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#define CACHE_LINE_SIZE 64
#define ALIGN_CACHE __attribute__((aligned(CACHE_LINE_SIZE)))

typedef enum {
    NODE_STATE_ALIVE = 0,
    NODE_STATE_SUSPECTED = 1,
    NODE_STATE_DEAD = 2,
    NODE_STATE_LEFT = 3
} NodeStateEnum;

typedef struct ALIGN_CACHE {
    uint8_t id[16];
    _Atomic NodeStateEnum state;
    _Atomic uint64_t suspicion_generation;
    _Atomic uint64_t expiration_timestamp;
    _Atomic uint64_t vector_clock;
    uint32_t probe_failures;
    uint32_t anti_entropy_weight;
} NodeState;

typedef struct ALIGN_CACHE {
    uint8_t* memory;
    size_t capacity;
    _Atomic size_t offset;
    pthread_mutex_t lock;
} ArenaAllocator;

ArenaAllocator* swim_arena_create(size_t capacity) {
    ArenaAllocator* arena = (ArenaAllocator*)aligned_alloc(CACHE_LINE_SIZE, sizeof(ArenaAllocator));
    if (!arena) return NULL;
    
    // Ensure capacity is a multiple of cache line size
    size_t aligned_capacity = (capacity + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
    arena->memory = (uint8_t*)aligned_alloc(CACHE_LINE_SIZE, aligned_capacity);
    if (!arena->memory) {
        free(arena);
        return NULL;
    }
    
    arena->capacity = aligned_capacity;
    atomic_init(&arena->offset, 0);
    pthread_mutex_init(&arena->lock, NULL);
    return arena;
}

void* swim_arena_alloc(ArenaAllocator* arena, size_t size) {
    if (!arena || size == 0) return NULL;
    
    // Align allocation size to 8 bytes for proper alignment on all architectures
    size_t aligned_size = (size + 7) & ~7;
    
    size_t current_offset = atomic_load(&arena->offset);
    while (true) {
        if (current_offset + aligned_size > arena->capacity) {
            return NULL; // Out of arena memory
        }
        if (atomic_compare_exchange_weak(&arena->offset, &current_offset, current_offset + aligned_size)) {
            return (void*)(arena->memory + current_offset);
        }
    }
}

void swim_arena_reset(ArenaAllocator* arena) {
    if (!arena) return;
    pthread_mutex_lock(&arena->lock);
    atomic_store(&arena->offset, 0);
    pthread_mutex_unlock(&arena->lock);
}

void swim_arena_destroy(ArenaAllocator* arena) {
    if (!arena) return;
    pthread_mutex_destroy(&arena->lock);
    free(arena->memory);
    free(arena);
}

NodeState* swim_nodestate_alloc(ArenaAllocator* arena, const uint8_t* node_id) {
    void* mem = swim_arena_alloc(arena, sizeof(NodeState));
    if (!mem) return NULL;
    
    NodeState* node = (NodeState*)mem;
    if (node_id) {
        memcpy(node->id, node_id, 16);
    } else {
        memset(node->id, 0, 16);
    }
    
    atomic_init(&node->state, NODE_STATE_ALIVE);
    atomic_init(&node->suspicion_generation, 0);
    atomic_init(&node->expiration_timestamp, 0);
    atomic_init(&node->vector_clock, 0);
    node->probe_failures = 0;
    node->anti_entropy_weight = 1;
    
    return node;
}