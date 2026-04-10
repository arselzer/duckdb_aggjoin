cmake_minimum_required(VERSION 3.12)

# Set extension name here
set(TARGET_NAME aggjoin)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)

set(EXTENSION_SOURCES
  src/aggjoin_extension.cpp
  src/aggjoin_logical.cpp
  src/aggjoin_optimizer.cpp
  src/aggjoin_physical.cpp
  src/aggjoin_emit.cpp
  src/aggjoin_emit_direct.cpp
  src/aggjoin_rewrite_build.cpp
  src/aggjoin_rewrite_final_bag.cpp
  src/aggjoin_rewrite_mixed.cpp
  src/aggjoin_sink.cpp
  src/aggjoin_source.cpp
  src/aggjoin_source_direct.cpp
  src/aggjoin_source_result_hash.cpp
  src/aggjoin_source_segmented.cpp
  src/aggjoin_rewrites.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
