#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <fcntl.h>

#define CACHE_LINE_SIZE 64
#define MAX_EVENTS 64
#define MAX_DATAGRAM_SIZE 1280
#define UUID_SIZE 16
#define MPSC_RING_SIZE 1024
#define ARENA_BLOCK_SIZE 65536

typedef enum {
    NODE_ALIVE = 0,
    NODE_SUSPECTED = 1,
    NODE_DEAD = 2,
    NODE_LEFT = 3
} NodeLifeState;

typedef struct {
    uint8_t id[UUID_SIZE];
    uint64_t counter;
} VectorClockTuple;

typedef struct {
    VectorClockTuple *tuples;
    size_t count;
    size_t capacity;
} VectorClock;

struct __attribute__((aligned(CACHE_LINE_SIZE))) NodeState {
    uint8_t id[UUID_SIZE];
    _Atomic NodeLifeState life_state;
    _Atomic uint64_t suspicion_counter;
    _Atomic uint64_t expiration_timestamp;
    VectorClock vclock;
    uint32_t hash_probe_dist;
    bool active;
};

typedef struct {
    uint8_t data[MAX_DATAGRAM_SIZE];
    size_t length;
    uint32_t ttl;
} PiggybackMessage;

typedef struct {
    PiggybackMessage buffer[MPSC_RING_SIZE];
    _Atomic size_t head;
    _Atomic size_t tail;
} MPSCQueue;

typedef struct ArenaBlock {
    struct ArenaBlock *next;
    size_t used;
    size_t capacity;
    uint8_t memory[ARENA_BLOCK_SIZE];
} ArenaBlock;

typedef struct {
    ArenaBlock *current;
    ArenaBlock *head;
    pthread_mutex_t lock;
} ThreadArena;

typedef struct {
    int epoll_fd;
    int udp_socket;
    _Atomic bool running;
    pthread_t reactor_thread;
    pthread_t ticker_thread;
    MPSCQueue piggyback_queue;
    ThreadArena arena;
    struct NodeState *registry;
    size_t registry_capacity;
    pthread_rwlock_t registry_lock;
} SwimSystem;

static inline uint64_t get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

static uint32_t hash_uuid(const uint8_t *id) {
    uint32_t hash = 2166136261u;
    for (int i = 0; i < UUID_SIZE; i++) {
        hash ^= id[i];
        hash *= 16777619u;
    }
    return hash;
}

void swim_arena_init(ThreadArena *arena) {
    arena->head = (ArenaBlock *)malloc(sizeof(ArenaBlock));
    arena->head->next = NULL;
    arena->head->used = 0;
    arena->head->capacity = ARENA_BLOCK_SIZE;
    arena->current = arena->head;
    pthread_mutex_init(&arena->lock, NULL);
}

void *swim_arena_alloc(ThreadArena *arena, size_t size) {
    size_t aligned_size = (size + 7) & ~7;
    pthread_mutex_lock(&arena->lock);
    
    if (arena->current->used + aligned_size > arena->current->capacity) {
        ArenaBlock *new_block = (ArenaBlock *)malloc(sizeof(ArenaBlock));
        new_block->next = NULL;
        new_block->used = 0;
        new_block->capacity = ARENA_BLOCK_SIZE;
        arena->current->next = new_block;
        arena->current = new_block;
    }
    
    void *ptr = &arena->current->memory[arena->current->used];
    arena->current->used += aligned_size;
    pthread_mutex_unlock(&arena->lock);
    return ptr;
}

void swim_arena_reset(ThreadArena *arena) {
    pthread_mutex_lock(&arena->lock);
    ArenaBlock *block = arena->head;
    while (block != NULL) {
        block->used = 0;
        block = block->next;
    }
    arena->current = arena->head;
    pthread_mutex_unlock(&arena->lock);
}

void swim_arena_destroy(ThreadArena *arena) {
    pthread_mutex_lock(&arena->lock);
    ArenaBlock *block = arena->head;
    while (block != NULL) {
        ArenaBlock *next = block->next;
        free(block);
        block = next;
    }
    pthread_mutex_unlock(&arena->lock);
    pthread_mutex_destroy(&arena->lock);
}

void swim_mpsc_init(MPSCQueue *q) {
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
}

bool swim_mpsc_push(MPSCQueue *q, const PiggybackMessage *msg) {
    size_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    size_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    
    if ((tail - head) >= MPSC_RING_SIZE) {
        return false;
    }
    
    q->buffer[tail % MPSC_RING_SIZE] = *msg;
    atomic_store_explicit(&q->tail, tail + 1, memory_order_release);
    return true;
}

bool swim_mpsc_pop(MPSCQueue *q, PiggybackMessage *msg) {
    size_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    
    if (head == tail) {
        return false;
    }
    
    *msg = q->buffer[head % MPSC_RING_SIZE];
    atomic_store_explicit(&q->head, head + 1, memory_order_release);
    return true;
}

bool swim_vector_clock_merge(VectorClock *dst, const VectorClock *src) {
    for (size_t i = 0; i < src->count; i++) {
        bool found = false;
        for (size_t j = 0; j < dst->count; j++) {
            if (memcmp(dst->tuples[j].id, src->tuples[i].id, UUID_SIZE) == 0) {
                if (src->tuples[i].counter > dst->tuples[j].counter) {
                    dst->tuples[j].counter = src->tuples[i].counter;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            if (dst->count >= dst->capacity) {
                size_t new_cap = dst->capacity == 0 ? 4 : dst->capacity * 2;
                VectorClockTuple *new_tuples = (VectorClockTuple *)realloc(dst->tuples, new_cap * sizeof(VectorClockTuple));
                if (!new_tuples) return false;
                dst->tuples = new_tuples;
                dst->capacity = new_cap;
            }
            dst->tuples[dst->count++] = src->tuples[i];
        }
    }
    return true;
}

void swim_node_state_init(struct NodeState *node, const uint8_t *id) {
    memcpy(node->id, id, UUID_SIZE);
    atomic_init(&node->life_state, NODE_ALIVE);
    atomic_init(&node->suspicion_counter, 0);
    atomic_init(&node->expiration_timestamp, 0);
    node->vclock.tuples = NULL;
    node->vclock.count = 0;
    node->vclock.capacity = 0;
    node->hash_probe_dist = 0;
    node->active = true;
}

void swim_registry_init(SwimSystem *sys, size_t capacity) {
    sys->registry_capacity = capacity;
    sys->registry = (struct NodeState *)calloc(capacity, sizeof(struct NodeState));
    pthread_rwlock_init(&sys->registry_lock, NULL);
}

bool swim_registry_insert(SwimSystem *sys, const uint8_t *id) {
    pthread_rwlock_wrlock(&sys->registry_lock);
    uint32_t hash = hash_uuid(id);
    size_t idx = hash % sys->registry_capacity;
    size_t probe = 0;
    
    struct NodeState new_node;
    swim_node_state_init(&new_node, id);
    new_node.hash_probe_dist = 0;

    while (sys->registry[idx].active) {
        if (memcmp(sys->registry[idx].id, id, UUID_SIZE) == 0) {
            pthread_rwlock_unlock(&sys->registry_lock);
            return false;
        }
        if (sys->registry[idx].hash_probe_dist < probe) {
            struct NodeState temp = sys->registry[idx];
            sys->registry[idx] = new_node;
            new_node = temp;
            probe = sys->registry[idx].hash_probe_dist;
        }
        idx = (idx + 1) % sys->registry_capacity;
        probe++;
        new_node.hash_probe_dist = probe;
        if (probe >= sys->registry_capacity) {
            pthread_rwlock_unlock(&sys->registry_lock);
            return false;
        }
    }
    
    sys->registry[idx] = new_node;
    pthread_rwlock_unlock(&sys->registry_lock);
    return true;
}

struct NodeState *swim_registry_lookup(SwimSystem *sys, const uint8_t *id) {
    pthread_rwlock_rdlock(&sys->registry_lock);
    uint32_t hash = hash_uuid(id);
    size_t idx = hash % sys->registry_capacity;
    size_t probe = 0;

    while (sys->registry[idx].active) {
        if (sys->registry[idx].hash_probe_dist < probe) {
            break;
        }
        if (memcmp(sys->registry[idx].id, id, UUID_SIZE) == 0) {
            pthread_rwlock_unlock(&sys->registry_lock);
            return &sys->registry[idx];
        }
        idx = (idx + 1) % sys->registry_capacity;
        probe++;
        if (probe >= sys->registry_capacity) {
            break;
        }
    }
    pthread_rwlock_unlock(&sys->registry_lock);
    return NULL;
}

bool swim_registry_remove(SwimSystem *sys, const uint8_t *id) {
    pthread_rwlock_wrlock(&sys->registry_lock);
    uint32_t hash = hash_uuid(id);
    size_t idx = hash % sys->registry_capacity;
    size_t probe = 0;
    size_t found_idx = (size_t)-1;

    while (sys->registry[idx].active) {
        if (sys->registry[idx].hash_probe_dist < probe) {
            break;
        }
        if (memcmp(sys->registry[idx].id, id, UUID_SIZE) == 0) {
            found_idx = idx;
            break;
        }
        idx = (idx + 1) % sys->registry_capacity;
        probe++;
        if (probe >= sys->registry_capacity) break;
    }

    if (found_idx == (size_t)-1) {
        pthread_rwlock_unlock(&sys->registry_lock);
        return false;
    }

    size_t current = found_idx;
    size_t next = (current + 1) % sys->registry_capacity;

    while (sys->registry[next].active && sys->registry[next].hash_probe_dist > 0) {
        sys->registry[current] = sys->registry[next];
        sys->registry[current].hash_probe_dist--;
        current = next;
        next = (current + 1) % sys->registry_capacity;
    }

    sys->registry[current].active = false;
    free(sys->registry[current].vclock.tuples);
    pthread_rwlock_unlock(&sys->registry_lock);
    return true;
}

static void *ticker_worker(void *arg) {
    SwimSystem *sys = (SwimSystem *)arg;
    while (atomic_load(&sys->running)) {
        usleep(100000); 
        uint64_t now = get_time_ms();
        
        pthread_rwlock_wrlock(&sys->registry_lock);
        for (size_t i = 0; i < sys->registry_capacity; i++) {
            if (!sys->registry[i].active) continue;
            NodeLifeState state = atomic_load(&sys->registry[i].life_state);
            if (state == NODE_SUSPECTED) {
                uint64_t exp = atomic_load(&sys->registry[i].expiration_timestamp);
                if (now >= exp) {
                    atomic_store(&sys->registry[i].life_state, NODE_DEAD);
                }
            }
        }
        pthread_rwlock_unlock(&sys->registry_lock);
        swim_arena_reset(&sys->arena);
    }
    return NULL;
}

static void *reactor_worker(void *arg) {
    SwimSystem *sys = (SwimSystem *)arg;
    struct epoll_event events[MAX_EVENTS];

    while (atomic_load(&sys->running)) {
        int n = epoll_wait(sys->epoll_fd, events, MAX_EVENTS, 100);
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == sys->udp_socket) {
                uint8_t rx_buffer[MAX_DATAGRAM_SIZE];
                struct sockaddr_in cli_addr;
                socklen_t addr_len = sizeof(cli_addr);
                ssize_t bytes = recvfrom(sys->udp_socket, rx_buffer, sizeof(rx_buffer), 0,
                                         (struct sockaddr *)&cli_addr, &addr_len);
                if (bytes > 0) {
                    PiggybackMessage msg;
                    if (bytes <= MAX_DATAGRAM_SIZE) {
                        memcpy(msg.data, rx_buffer, bytes);
                        msg.length = bytes;
                        msg.ttl = 3;
                        swim_mpsc_push(&sys->piggyback_queue, &msg);
                    }
                }
            }
        }
    }
    return NULL;
}

bool swim_system_init(SwimSystem *sys, int port, size_t registry_capacity) {
    atomic_store(&sys->running, true);
    swim_mpsc_init(&sys->piggyback_queue);
    swim_arena_init(&sys->arena);
    swim_registry_init(sys, registry_capacity);

    sys->udp_socket = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
    if (sys->udp_socket < 0) return false;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(sys->udp_socket, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sys->udp_socket);
        return false;
    }

    sys->epoll_fd = epoll_