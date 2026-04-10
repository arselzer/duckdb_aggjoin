.DEFAULT_GOAL := all

# DuckDB source directory (gitignored, clone with: git clone --depth 1 --branch v1.5.1 https://github.com/duckdb/duckdb.git duckdb)
DUCKDB_DIR ?= duckdb
EXT_NAME := aggjoin
DUCKDB_VERSION ?= v1.4.3

# Build type (Release or Debug)
BUILD_TYPE ?= Release

.PHONY: all clean test wasm debug

EXT_NAME_UPPER := $(shell echo $(EXT_NAME) | tr a-z A-Z)

# --- Native build (for testing) ---

all:
	@# IMPORTANT: touch source to force recompilation (cmake may silently skip)
	touch src/aggjoin_optimizer.cpp
	mkdir -p build/$(BUILD_TYPE) && \
	cd build/$(BUILD_TYPE) && \
	cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
	      -DEXTENSION_STATIC_BUILD=1 \
	      -DDUCKDB_EXTENSION_NAMES="$(EXT_NAME)" \
	      -DDUCKDB_EXTENSION_$(EXT_NAME_UPPER)_PATH="$(CURDIR)" \
	      -DDUCKDB_EXTENSION_$(EXT_NAME_UPPER)_INCLUDE_PATH="$(CURDIR)/src/include" \
	      -DDUCKDB_EXTENSION_$(EXT_NAME_UPPER)_TEST_PATH="$(CURDIR)/test" \
	      -DDUCKDB_EXTENSION_$(EXT_NAME_UPPER)_LOAD_TESTS=1 \
	      -DDUCKDB_EXTENSION_$(EXT_NAME_UPPER)_SHOULD_LINK=1 \
	      ../../$(DUCKDB_DIR) && \
	cmake --build . --config $(BUILD_TYPE) -j

debug:
	$(MAKE) BUILD_TYPE=Debug all

test: all
	cd build/$(BUILD_TYPE) && \
	./test/unittest "$(CURDIR)/test/sql/aggjoin.test"

# --- WASM build (for DuckDB-WASM frontend) ---
# Requires: Emscripten 3.1.71, DuckDB source in $(DUCKDB_DIR)
# Builds a loadable .duckdb_extension.wasm, patches metadata, deploys to frontend.

wasm:
	DUCKDB_DIR="$(CURDIR)/$(DUCKDB_DIR)" DUCKDB_VERSION="$(DUCKDB_VERSION)" \
	  ./scripts/build_wasm.sh

wasm-build-only:
	DUCKDB_DIR="$(CURDIR)/$(DUCKDB_DIR)" DUCKDB_VERSION="$(DUCKDB_VERSION)" \
	  ./scripts/build_wasm.sh --build-only

# --- Cleanup ---

clean:
	rm -rf build
