# 🚀 AnuDBBenchmark
This repository benchmarks the performance of [AnuDB](https://github.com/hash-anu/AnuDB), a custom storage engine designed for high-efficiency data operations, in comparison with SQLite3.

⚠️ Disclaimer:This benchmark focuses on specific workloads (insert, query, update, delete, and parallel operations using JSON documents) executed on embedded hardware (e.g., Raspberry Pi).It is not a general-purpose performance comparison.SQLite is a feature-rich, general-purpose SQL database optimized for reliability and transactional correctness across a wide range of use cases.AnuDB is a lightweight engine optimized for high-throughput workloads with a different design philosophy.Results may vary based on workload characteristics, hardware, and tuning parameters.
---
## 📊 Benchmark Results

The following table shows the performance metrics comparing AnuDB and SQLite3:

| Database | Operation | Time(s) | Operations | Ops/s |
|----------|-----------|---------|------------|-------|
| AnuDB    | Insert    | 33.776  | 10000      | 296.068 |
| AnuDB    | Query     | 29.09   | 1000       | 34.3761 |
| AnuDB    | Update    | 17.176  | 5000       | 291.104 |
| AnuDB    | Delete    | 7.306   | 2500       | 342.185 |
| AnuDB    | Parallel  | 1429.33 | 10000      | 6.99629 |
| SQLite3  | Insert    | 8.659   | 10000      | 1154.87 |
| SQLite3  | Query     | 35.857  | 1000       | 27.8886 |
| SQLite3  | Update    | 8.263   | 5000       | 605.107 |
| SQLite3  | Delete    | 1.374   | 2500       | 1819.51 |
| SQLite3  | Parallel  | 835.498 | 2500       | 2.99223 |

### 🔍 Performance Analysis

#### Insert Operations
- **SQLite3**: 1154.87 ops/s
- **AnuDB**: 296.068 ops/s
- SQLite3 is approximately 3.9x faster at inserts

#### Query Operations
- **SQLite3**: 27.8886 ops/s
- **AnuDB**: 34.3761 ops/s
- AnuDB is approximately 1.23x faster at queries

#### Update Operations
- **SQLite3**: 605.107 ops/s
- **AnuDB**: 291.104 ops/s
- SQLite3 is approximately 2.1x faster at updates

#### Delete Operations
- **SQLite3**: 1819.51 ops/s
- **AnuDB**: 342.185 ops/s
- SQLite3 is approximately 5.3x faster at deletes

#### Parallel Operations
- **SQLite3**: 2.99223 ops/s (2,500 operations)
- **AnuDB**: 6.99629 ops/s (10,000 operations)
- AnuDB processed 4x more operations and achieved approximately 2.34x higher ops/s

### 📈 Summary
- **SQLite3** excels at basic CRUD operations, particularly inserts and deletes
- **AnuDB** performs better in queries and parallel operations
- The choice between these databases would depend on your specific workload patterns

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
OR
# Run below bash command to compare Sqlite with AnuDB
./build_anusqlite.sh
```
If no path is given, it defaults to `../AnuDB`.
### 4️⃣ Run the Benchmark
```bash
./benchmark
```
---
## 📈 Benchmark Environment
- Hardware: Raspberry Pi
- Both AnuDB and SQLite3 were tested under identical conditions
---
## 📚 More Details
For methodology and performance insights, please refer to our blog:  
👉 [Medium](https://medium.com/@hashmak.jsn/absolutely-ca702d276a08)

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

If you build anusqlite_benchmark then below output will be shown:

 ./benchmark
AnuDB Load Testing Benchmark
=======================================
===== Database Benchmark: AnuDB vs SQLITE3  =====
Configuration:
- Documents: 10000
- Queries: 1000
- Parallel Threads: 4

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

Running SQLite3 tests...
  Running insert test...
  Running query test...
  Running update test...
  Running delete test...
  Running parallel operations test...
Thread 0 failed to commit transaction: cannot commit transaction - SQL statements in progress
Thread 1 failed to commit transaction: cannot commit transaction - SQL statements in progress
Thread 2 failed to commit transaction: cannot commit transaction - SQL statements in progress
Completed SQLite3 tests.


===== Benchmark Results =====
Operation           AnuDB Time(s)  AnuDB Ops      AnuDB Ops/s    SQLite3 Time(s)SQLite3 Ops    SQLite3 Ops/s
Insert              33.776         10000          296.1          8.659          10000          1154.9
Query               29.090         1000           34.4           35.857         1000           27.9
Update              17.176         5000           291.1          8.263          5000           605.1
Delete              7.306          2500           342.2          1.374          2500           1819.5
Parallel            1429.328       10000          7.0            835.498        2500           3.0

===== Performance Comparison =====
Ratio of AnuDB to SQLite3 (higher means AnuDB is faster)
Operation           Time Ratio     Ops/s Ratio
Insert              0.26           0.26
Query               1.23           1.23
Update              0.48           0.48
Delete              0.19           0.19
Parallel            0.58           2.34

Benchmark results saved to 'benchmark_results.csv'
```
---
## 🔧 Troubleshooting
- Ensure `g++`, `cmake`, and necessary libraries are installed.
- Use absolute paths to avoid file resolution issues.
