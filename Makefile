CC ?= gcc
CFLAGS ?= -std=c99 -O3 -Wall -Wextra -Iinclude
LDFLAGS ?= -pthread

SRC_DIR = src
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmark

TARGET = $(BUILD_DIR)/libswim_gossip_core.a
TEST_TARGET = $(BUILD_DIR)/test_runner
BENCH_TARGET = $(BUILD_DIR)/bench_runner

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.c)
BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.c)

.PHONY: all build test benchmark clean

all: build

build: $(BUILD_DIR) $(TARGET)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(TARGET): $(OBJS)
	@if [ -n "$(OBJS)" ]; then \
		ar rcs $@ $(OBJS); \
	else \
		touch $@; \
	fi

test: build
	@mkdir -p $(BUILD_DIR)
	@if [ -n "$(TEST_SRCS)" ]; then \
		$(CC) $(CFLAGS) $(TEST_SRCS) $(TARGET) $(LDFLAGS) -o $(TEST_TARGET) && $(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR)/. Skipping."; \
	fi

benchmark: build
	@mkdir -p $(BUILD_DIR)
	@if [ -n "$(BENCH_SRCS)" ]; then \
		$(CC) $(CFLAGS) $(BENCH_SRCS) $(TARGET) $(LDFLAGS) -o $(BENCH_TARGET) && $(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR)/. Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)