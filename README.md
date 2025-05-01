# 🚀 AnuDBBenchmark

This repository benchmarks the performance of [AnuDB](https://github.com/hash-anu/AnuDB), a custom storage engine designed for high-efficiency data operations.

---

## 🛠️ Setup Instructions

### 1️⃣ Clone and Build AnuDB

```bash

# Clone the repository
git clone https://github.com/hash-anu/AnuDB.git
cd AnuDB/third_party/nanomq/
git submodule update --init --recursive
cd ../..
mkdir build
cd build
cmake ..
make

or
# Configure with ZSTD compression support
cmake -DZSTD_INCLUDE_DIR=/path/to/zstd/include -DZSTD_LIB_DIR=/path/to/zstd/lib ..
make
```

### 2️⃣ Clone This Benchmark Repository

```bash
git clone https://github.com/hash-anu/AnuDBBenchmark.git
cd AnuDBBenchmark
```

### 3️⃣ Build the Benchmark

Use the `build.sh` script to compile the benchmark. You can optionally specify the path to the AnuDB repository:

```bash
./build.sh /full/path/to/AnuDB
```

If no path is given, it defaults to `../AnuDB`.

### 4️⃣ Run the Benchmark

```bash
./benchmark
```

---

## 📈 Benchmark Notes

- This benchmark evaluates only **AnuDB**.
- Performance metrics include average write/read times and throughput.
- Results may vary depending on your platform and build settings.

---

## 📚 More Details

For methodology and performance insights, please refer to our blog:  
👉 

---

## 🤩 Example Output

```
AnuDB Load Testing Benchmark
=======================================
===== Database Benchmark: AnuDB  =====
Configuration:
- Documents: 10000
- Queries: 1000
- Parallel Threads: 10

Running AnuDB tests...
Index created successfully!!!
Index created successfully!!!
Index created successfully!!!
Index created successfully!!!
Index created successfully!!!
  Running insert test...
  Running query test...
  Running update test...
  Running delete test...
  Running parallel operations test...
Completed AnuDB tests.


===== Benchmark Results =====
Operation           AnuDB Time(s)  AnuDB Ops      AnuDB Ops/s
Insert              0.977          10000          10235.4
Query               2.870          1000           348.4
Update              0.445          5000           11236.0
Delete              0.143          2500           17482.5
Parallel            0.648          10000          15432.1

Benchmark results saved to 'benchmark_results.csv'
```

---

## 🔧 Troubleshooting

- Ensure `g++`, `cmake`, and necessary libraries are installed.
- Use absolute paths to avoid file resolution issues.
