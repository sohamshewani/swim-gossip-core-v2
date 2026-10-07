#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <immintrin.h>

#define SWIM_MAX_VECTOR_NODES 256
#define SWIM_NODE_ID_LEN 16

typedef struct {
    uint8_t id[SWIM_NODE_ID_LEN];
} __attribute__((aligned(8))) swim_node_id_t;

typedef struct {
    swim_node_id_t node_id;
    uint64_t counter;
} __attribute__((aligned(16))) swim_vector_element_t;

typedef struct {
    uint32_t capacity;
    _Atomic uint32_t size;
    swim_vector_element_t elements[SWIM_MAX_VECTOR_NODES];
} __attribute__((aligned(64))) swim_vector_clock_t;

typedef enum {
    SWIM_CAUSAL_BEFORE = -1,
    SWIM_CAUSAL_CONCURRENT = 0,
    SWIM_CAUSAL_AFTER = 1,
    SWIM_CAUSAL_EQUAL = 2
} swim_causal_relation_t;

static inline int swim_node_id_compare(const swim_node_id_t *a, const swim_node_id_t *b) {
    return memcmp(a->id, b->id, SWIM_NODE_ID_LEN);
}

void swim_vector_clock_init(swim_vector_clock_t *vc) {
    if (!vc) return;
    atomic_store_explicit(&vc->size, 0, memory_order_relaxed);
    vc->capacity = SWIM_MAX_VECTOR_NODES;
    memset(vc->elements, 0, sizeof(vc->elements));
}

void swim_vector_clock_destroy(swim_vector_clock_t *vc) {
    if (!vc) return;
    atomic_store_explicit(&vc->size, 0, memory_order_relaxed);
}

bool swim_vector_clock_copy(swim_vector_clock_t *dest, const swim_vector_clock_t *src) {
    if (!dest || !src) return false;
    uint32_t src_size = atomic_load_explicit(&src->size, memory_order_relaxed);
    if (src_size > SWIM_MAX_VECTOR_NODES) return false;

    memcpy(dest->elements, src->elements, src_size * sizeof(swim_vector_element_t));
    atomic_store_explicit(&dest->size, src_size, memory_order_release);
    dest->capacity = src->capacity;
    return true;
}

uint64_t swim_vector_clock_get(const swim_vector_clock_t *vc, const swim_node_id_t *node_id) {
    if (!vc || !node_id) return 0;
    uint32_t size = atomic_load_explicit((_Atomic uint32_t *)&vc->size, memory_order_acquire);
    
    for (uint32_t i = 0; i < size; ++i) {
        if (swim_node_id_compare(&vc->elements[i].node_id, node_id) == 0) {
            return vc->elements[i].counter;
        }
    }
    return 0;
}

bool swim_vector_clock_increment(swim_vector_clock_t *vc, const swim_node_id_t *node_id) {
    if (!vc || !node_id) return false;

    uint32_t size = atomic_load_explicit(&vc->size, memory_order_acquire);
    uint32_t i = 0;

    for (i = 0; i < size; ++i) {
        if (swim_node_id_compare(&vc->elements[i].node_id, node_id) == 0) {
            vc->elements[i].counter++;
            atomic_thread_fence(memory_order_release);
            return true;
        }
    }

    while (true) {
        if (size >= SWIM_MAX_VECTOR_NODES) {
            return false;
        }

        uint32_t expected = size;
        if (atomic_compare_exchange_weak_explicit(&vc->size, &expected, size + 1,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
            memcpy(&vc->elements[expected].node_id, node_id, sizeof(swim_node_id_t));
            vc->elements[expected].counter = 1;
            return true;
        }
        size = expected;
        for (i = 0; i < size; ++i) {
            if (swim_node_id_compare(&vc->elements[i].node_id, node_id) == 0) {
                vc->elements[i].counter++;
                atomic_thread_fence(memory_order_release);
                return true;
            }
        }
    }
}

bool swim_vector_clock_set(swim_vector_clock_t *vc, const swim_node_id_t *node_id, uint64_t counter) {
    if (!vc || !node_id) return false;

    uint32_t size = atomic_load_explicit(&vc->size, memory_order_acquire);
    uint32_t i = 0;

    for (i = 0; i < size; ++i) {
        if (swim_node_id_compare(&vc->elements[i].node_id, node_id) == 0) {
            if (counter > vc->elements[i].counter) {
                vc->elements[i].counter = counter;
                atomic_thread_fence(memory_order_release);
            }
            return true;
        }
    }

    while (true) {
        if (size >= SWIM_MAX_VECTOR_NODES) {
            return false;
        }

        uint32_t expected = size;
        if (atomic_compare_exchange_weak_explicit(&vc->size, &expected, size + 1,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
            memcpy(&vc->elements[expected].node_id, node_id, sizeof(swim_node_id_t));
            vc->elements[expected].counter = counter;
            return true;
        }
        size = expected;
        for (i = 0; i < size; ++i) {
            if (swim_node_id_compare(&vc->elements[i].node_id, node_id) == 0) {
                if (counter > vc->elements[i].counter) {
                    vc->elements[i].counter = counter;
                    atomic_thread_fence(memory_order_release);
                }
                return true;
            }
        }
    }
}

swim_causal_relation_t swim_vector_clock_compare(const swim_vector_clock_t *a, const swim_vector_clock_t *b) {
    if (!a || !b) return SWIM_CAUSAL_CONCURRENT;

    uint32_t size_a = atomic_load_explicit((_Atomic uint32_t *)&a->size, memory_order_acquire);
    uint32_t size_b = atomic_load_explicit((_Atomic uint32_t *)&b->size, memory_order_acquire);

    bool a_less_or_eq = true;
    bool b_less_or_eq = true;
    bool any_difference = false;

    // Collect all unique node IDs from both clocks to ensure total coverage
    swim_node_id_t all_nodes[SWIM_MAX_VECTOR_NODES * 2];
    uint32_t total_nodes = 0;

    for (uint32_t i = 0; i < size_a; ++i) {
        all_nodes[total_nodes++] = a->elements[i].node_id;
    }
    for (uint32_t i = 0; i < size_b; ++i) {
        bool found = false;
        for (uint32_t j = 0; j < size_a; ++j) {
            if (swim_node_id_compare(&b->elements[i].node_id, &a->elements[j].node_id) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            all_nodes[total_nodes++] = b->elements[i].node_id;
        }
    }

    for (uint32_t i = 0; i < total_nodes; ++i) {
        uint64_t val_a = swim_vector_clock_get(a, &all_nodes[i]);
        uint64_t val_b = swim_vector_clock_get(b, &all_nodes[i]);

        if (val_a > val_b) {
            b_less_or_eq = false;
            any_difference = true;
        } else if (val_a < val_b) {
            a_less_or_eq = false;
            any_difference = true;
        }
    }

    if (!any_difference) {
        return SWIM_CAUSAL_EQUAL;
    }
    if (a_less_or_eq && !b_less_or_eq) {
        return SWIM_CAUSAL_BEFORE;
    }
    if (!a_less_or_eq && b_less_or_eq) {
        return SWIM_CAUSAL_AFTER;
    }
    return SWIM_CAUSAL_CONCURRENT;
}

bool swim_vector_clock_merge(swim_vector_clock_t *dest, const swim_vector_clock_t *src) {
    if (!dest || !src) return false;

    uint32_t src_size = atomic_load_explicit((_Atomic uint32_t *)&src->size, memory_order_acquire);
    
    for (uint32_t i = 0; i < src_size; ++i) {
        uint64_t src_val = src->elements[i].counter;
        uint64_t dest_val = swim_vector_clock_get(dest, &src->elements[i].node_id);
        
        if (src_val > dest_val) {
            if (!swim_vector_clock_set(dest, &src->elements[i].node_id, src_val)) {
                return false;
            }
        }
    }
    return true;
}