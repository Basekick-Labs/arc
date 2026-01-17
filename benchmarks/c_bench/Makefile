# C Benchmark Client Makefile
#
# Requires: libcurl, msgpack-c
#   brew install curl msgpack-c

CC = clang
CFLAGS = -O3 -Wall -Wextra -march=native -flto
LDFLAGS = -lcurl -lpthread -lmsgpackc

# macOS homebrew locations
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    BREW_PREFIX := $(shell brew --prefix 2>/dev/null)
    ifneq ($(BREW_PREFIX),)
        CFLAGS += -I$(BREW_PREFIX)/include
        LDFLAGS += -L$(BREW_PREFIX)/lib
    endif
endif

TARGET = sustained_bench
SRC = sustained_bench.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo ""
	@echo "Build complete: $(TARGET)"
	@echo ""
	@echo "Usage:"
	@echo "  ./$(TARGET) -d 30 -w 100 -b 1000"
	@echo "  ./$(TARGET) -d 60 -w 200 -b 5000 -t financial"

clean:
	rm -f $(TARGET)
