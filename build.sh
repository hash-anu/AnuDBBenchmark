#!/bin/bash

# Usage: ./build.sh /path/to/AnuDB /path/to/sqlite
# Default path to ../AnuDB and ../sqlite if not provided
ANUDB_PATH=${1:-../AnuDB}
SQLITE3_PATH=${2:-../sqlite}

$HOME/rpi-tools/arm-bcm2708/arm-linux-gnueabihf/bin/arm-linux-gnueabihf-g++ -std=c++11 anusqlite_benchmark.cpp \
  -I"$ANUDB_PATH/src/" \
  -I"$ANUDB_PATH/src/storage_engine/" \
  -I"$ANUDB_PATH/third_party/json/" \
  -I"$ANUDB_PATH/third_party/rocksdb/include/" \
  -I"$SQLITE3_PATH/sqlite-install-arm/include" \
  -L"$ANUDB_PATH/build-arm/" \
  -L"$SQLITE3_PATH/sqlite-install-arm/lib/" \
  "$SQLITE3_PATH/sqlite-install-arm/lib/libsqlite3.a" \
  "$ANUDB_PATH/build-arm/src/storage_engine/liblibstorage.a" \
  "$ANUDB_PATH/build-arm/third_party/rocksdb/librocksdb.a" \
  "$ANUDB_PATH/build-arm/liblibanu.a" \
  -pthread -ldl -o benchmark

