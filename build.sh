#!/bin/bash

# Usage: ./build.sh /path/to/AnuDB
# Default path to ../AnuDB if not provided
ANUDB_PATH=${1:-../AnuDB}

g++ -std=c++11 anudb_benchmark.cpp \
  -I"$ANUDB_PATH/src/" \
  -I"$ANUDB_PATH/src/storage_engine/" \
  -I"$ANUDB_PATH/third_party/json/" \
  -I"$ANUDB_PATH/third_party/rocksdb/include/" \
  -L"$ANUDB_PATH/build/" \
  "$ANUDB_PATH/build/src/storage_engine/liblibstorage.a" \
  "$ANUDB_PATH/build/third_party/rocksdb/librocksdb.a" \
  -llibanu \
  -o benchmark
