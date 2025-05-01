#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <fstream>
#include <iomanip>
#include <memory>
#include <algorithm>
#include <functional>
#include <sstream>
#include <thread>
#include <mutex>
#include <atomic>

// AnuDB includes
#include "Database.h"
#include "json.hpp"

using json = nlohmann::json;
using namespace std::chrono;
#include <memory>

namespace std {
    template <typename T, typename... Args>
    std::unique_ptr<T> make_unique(Args&&... args) {
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
}

// Configuration constants
const int NUM_DOCUMENTS = 10000;          // Number of documents to insert in each test
const int NUM_QUERIES = 1000;             // Number of queries to execute in each test
const int NUM_THREADS = 10;                // Number of concurrent threads for parallel tests
const std::string DB_PATH_ANUDB = "./benchmark_anudb";
const std::string DB_PATH_SQLITE = "./benchmark_sqlite.db";
const std::string COLLECTION_NAME = "products";

// Test case class for common functionality
class BenchmarkTest {
public:
    BenchmarkTest(const std::string& name) : testName(name) {}
    virtual ~BenchmarkTest() {}
    
    virtual bool setup() = 0;
    virtual bool cleanup() = 0;
    virtual bool runInsertTest() = 0;
    virtual bool runQueryTest() = 0;
    virtual bool runUpdateTest() = 0;
    virtual bool runDeleteTest() = 0;
    virtual bool runParallelTest() = 0;
    
    const std::string& getName() const { return testName; }
    
    // Results storage
    struct TestResult {
        double insertTime = 0;
        double queryTime = 0;
        double updateTime = 0;
        double deleteTime = 0;
        double parallelTime = 0;
        size_t insertOps = 0;
        size_t queryOps = 0;
        size_t updateOps = 0;
        size_t deleteOps = 0;
        size_t parallelOps = 0;
    };
    
    TestResult results;

protected:
    std::string testName;
    
    // Helper method to generate random product data
    json generateRandomProduct(int index) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_real_distribution<> price_dist(10.0, 2000.0);
        static std::uniform_int_distribution<> stock_dist(0, 500);
        static std::uniform_real_distribution<> rating_dist(1.0, 5.0);
        static std::uniform_int_distribution<> category_dist(0, 3);
        
        const std::vector<std::string> categories = {"Electronics", "Books", "Food", "Clothing"};
        const std::vector<std::string> brands = {"TechMaster", "ReadBooks", "FoodDelight", "FashionStyle"};
        
        int category_idx = category_dist(gen);
        double price = std::round(price_dist(gen) * 100.0) / 100.0;
        int stock = stock_dist(gen);
        double rating = std::round(rating_dist(gen) * 10.0) / 10.0;
        
        json product = {
            {"id", "prod" + std::to_string(index)},
            {"name", "Product " + std::to_string(index)},
            {"price", price},
            {"stock", stock},
            {"category", categories[category_idx]},
            {"brand", brands[category_idx]},
            {"rating", rating},
            {"available", (stock > 0)},
            {"created_at", getCurrentTimeString()}
        };
        
        // Add category-specific attributes
        switch (category_idx) {
            case 0: // Electronics
                product["specs"] = {
                    {"processor", "i" + std::to_string(5 + (index % 5))},
                    {"ram", std::to_string(4 * (1 + (index % 4))) + "GB"},
                    {"storage", std::to_string(128 * (1 + (index % 8))) + "GB"}
                };
                break;
            case 1: // Books
                product["author"] = "Author " + std::to_string(1 + (index % 20));
                product["pages"] = 100 + (index % 500);
                product["publisher"] = "Publisher " + std::to_string(1 + (index % 10));
                break;
            case 2: // Food
                product["expiry_date"] = "2025-" + 
                    std::to_string(1 + (index % 12)) + "-" + 
                    std::to_string(1 + (index % 28));
                product["weight"] = std::to_string((index % 10) * 100) + "g";
                product["organic"] = (index % 2 == 0);
                break;
            case 3: // Clothing
                product["size"] = (index % 6 == 0) ? "XS" : 
                                 (index % 6 == 1) ? "S" : 
                                 (index % 6 == 2) ? "M" : 
                                 (index % 6 == 3) ? "L" : 
                                 (index % 6 == 4) ? "XL" : "XXL";
                product["color"] = (index % 7 == 0) ? "Red" : 
                                  (index % 7 == 1) ? "Blue" : 
                                  (index % 7 == 2) ? "Green" : 
                                  (index % 7 == 3) ? "Black" : 
                                  (index % 7 == 4) ? "White" : 
                                  (index % 7 == 5) ? "Yellow" : "Purple";
                product["material"] = (index % 4 == 0) ? "Cotton" : 
                                     (index % 4 == 1) ? "Polyester" : 
                                     (index % 4 == 2) ? "Wool" : "Silk";
                break;
        }
        
        return product;
    }
    
    // Get current time as formatted string
    std::string getCurrentTimeString() {
        auto now = system_clock::now();
        auto now_time_t = system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&now_time_t), "%Y-%m-%d %H:%M:%S");
        return ss.str();
    }
    
    // Timer function for benchmarking
    template<typename Func>
    double measureTime(Func&& func) {
        auto start = high_resolution_clock::now();
        func();
        auto end = high_resolution_clock::now();
        return duration_cast<milliseconds>(end - start).count() / 1000.0;
    }
};

// AnuDB test implementation
class AnuDBTest : public BenchmarkTest {
public:
    AnuDBTest() : BenchmarkTest("AnuDB") {}
    
    bool setup() override {
        db = std::make_unique<anudb::Database>(DB_PATH_ANUDB);
        auto status = db->open();
        if (!status.ok()) {
            std::cerr << "Failed to open AnuDB database: " << status.message() << std::endl;
            return false;
        }
        
        // Create collection
        status = db->createCollection(COLLECTION_NAME);
        if (!status.ok() && status.message().find("already exists") == std::string::npos) {
            std::cerr << "Failed to create collection: " << status.message() << std::endl;
            return false;
        }
        
        collection = db->getCollection(COLLECTION_NAME);
        if (!collection) {
            std::cerr << "Failed to get collection." << std::endl;
            return false;
        }
        
        // Create indexes
        for (const auto& field : {"price", "stock", "category", "rating", "available"}) {
            status = collection->createIndex(field);
            if (!status.ok() && status.message().find("already exists") == std::string::npos) {
                std::cerr << "Failed to create index on " << field << ": " << status.message() << std::endl;
                return false;
            }
        }
        
        return true;
    }
    
    bool cleanup() override {
        if (!db) return false;
        auto status = db->close();
        return status.ok();
    }
    
    bool runInsertTest() override {
        if (!collection) return false;
        
        int successCount = 0;
        results.insertTime = measureTime([&]() {
            for (int i = 0; i < NUM_DOCUMENTS; i++) {
                json productData = generateRandomProduct(i);
                std::string docId = "prod" + std::to_string(i);
                
                anudb::Document doc(docId, productData);
                auto status = collection->createDocument(doc);
                
                if (status.ok()) {
                    successCount++;
                }
            }
        });
        
        results.insertOps = successCount;
        return true;
    }
    
    bool runQueryTest() override {
        if (!collection) return false;
        
        int successCount = 0;
        std::vector<json> queries = {
            // Query by category
            {{"$eq", {{"category", "Electronics"}}}},
            {{"$eq", {{"category", "Books"}}}},
            {{"$eq", {{"category", "Food"}}}},
            {{"$eq", {{"category", "Clothing"}}}},
            
            // Query by price range
            {{"$gt", {{"price", 500.0}}}},
            {{"$lt", {{"price", 100.0}}}},
            {{"$and", {
                {{"$gt", {{"price", 100.0}}}},
                {{"$lt", {{"price", 500.0}}}}
            }}},
            
            // Query by rating
            {{"$gt", {{"rating", 4.0}}}},
            
            // Query by availability
            {{"$eq", {{"available", true}}}},
            
            // Combined queries
            {{"$and", {
                {{"$eq", {{"category", "Electronics"}}}},
                {{"$gt", {{"price", 1000.0}}}}
            }}}
        };
        
        results.queryTime = measureTime([&]() {
            for (int i = 0; i < NUM_QUERIES; i++) {
                const json& query = queries[i % queries.size()];
                std::vector<std::string> docIds = collection->findDocument(query);
                successCount += docIds.size() > 0 ? 1 : 0;
            }
        });
        
        results.queryOps = NUM_QUERIES;
        return true;
    }
    
    bool runUpdateTest() override {
        if (!collection) return false;
        
        int successCount = 0;
        results.updateTime = measureTime([&]() {
            for (int i = 0; i < NUM_DOCUMENTS / 2; i++) {
                std::string docId = "prod" + std::to_string(i);
                
                // Generate update data
                json updateData = {
                    {"$set", {
                        {"price", 100.0 + (i % 10) * 50.0},
                        {"stock", 10 + (i % 20)},
                        {"updated_at", getCurrentTimeString()}
                    }}
                };
                
                auto status = collection->updateDocument(docId, updateData);
                if (status.ok()) {
                    successCount++;
                }
            }
        });
        
        results.updateOps = successCount;
        return true;
    }
    
    bool runDeleteTest() override {
        if (!collection) return false;
        
        int successCount = 0;
        results.deleteTime = measureTime([&]() {
            for (int i = 0; i < NUM_DOCUMENTS / 4; i++) {
                std::string docId = "prod" + std::to_string(i * 3);  // Delete every third document
                
                auto status = collection->deleteDocument(docId);
                if (status.ok()) {
                    successCount++;
                }
            }
        });
        
        results.deleteOps = successCount;
        return true;
    }
    
    bool runParallelTest() override {
        if (!collection) return false;
        
        std::vector<std::thread> threads;
        std::atomic<int> successCount(0);
        std::mutex mtx;
        
        // Create a barrier to synchronize thread start
        std::atomic<int> barrier(0);
        
        auto start = high_resolution_clock::now();
        
        // Spawn threads
        for (int t = 0; t < NUM_THREADS; t++) {
            threads.emplace_back([&, t]() {
                // Wait until all threads are ready
                barrier.fetch_add(1);
                while (barrier.load() < NUM_THREADS) {
                    std::this_thread::yield();
                }
                
                int threadSuccessCount = 0;
                int docsPerThread = NUM_DOCUMENTS / NUM_THREADS;
                int startIdx = t * docsPerThread;
                int endIdx = startIdx + docsPerThread;
                
                // Each thread performs a mix of operations
                for (int i = startIdx; i < endIdx; i++) {
                    std::string docId = "parallel_prod" + std::to_string(i);
                    
                    // Insert
                    json productData = generateRandomProduct(i + 10000);  // Offset to avoid conflicts
                    anudb::Document doc(docId, productData);
                    auto status = collection->createDocument(doc);
                    if (status.ok()) {
                        threadSuccessCount++;
                    }
                    
                    // Query
                    if (i % 5 == 0) {
                        json query = {{"$eq", {{"category", productData["category"].get<std::string>()}}}};
                        collection->findDocument(query);
                    }
                    
                    // Update
                    if (i % 3 == 0) {
                        json updateData = {
                            {"$set", {
                                {"price", (double)productData["price"] * 1.1},
                                {"stock", i % 100},
                                {"updated_at", getCurrentTimeString()}
                            }}
                        };
                        collection->updateDocument(docId, updateData);
                    }
                    
                    // Delete
                    if (i % 7 == 0) {
                        collection->deleteDocument(docId);
                    }
                }
                
                successCount.fetch_add(threadSuccessCount);
            });
        }
        
        // Join all threads
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = high_resolution_clock::now();
        results.parallelTime = duration_cast<milliseconds>(end - start).count() / 1000.0;
        results.parallelOps = successCount.load();
        
        return true;
    }
    
private:
    std::unique_ptr<anudb::Database> db;
    anudb::Collection* collection = nullptr;
};

// Function to run all tests and print results
void runBenchmarks() {
    std::vector<std::unique_ptr<BenchmarkTest>> tests;
    tests.push_back(std::make_unique<AnuDBTest>());
    //tests.push_back(std::make_unique<SQLiteTest>());

    std::cout << "===== Database Benchmark: AnuDB  =====" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "- Documents: " << NUM_DOCUMENTS << std::endl;
    std::cout << "- Queries: " << NUM_QUERIES << std::endl;
    std::cout << "- Parallel Threads: " << NUM_THREADS << std::endl;
    std::cout << std::endl;

    for (auto& test : tests) {
        std::cout << "Running " << test->getName() << " tests..." << std::endl;

        // Setup test
        if (!test->setup()) {
            std::cerr << "Failed to set up " << test->getName() << " test." << std::endl;
            continue;
        }

        // Run insert test
        std::cout << "  Running insert test..." << std::endl;
        if (!test->runInsertTest()) {
            std::cerr << "Failed to run insert test for " << test->getName() << std::endl;
        }

        // Run query test
        std::cout << "  Running query test..." << std::endl;
        if (!test->runQueryTest()) {
            std::cerr << "Failed to run query test for " << test->getName() << std::endl;
        }

        // Run update test
        std::cout << "  Running update test..." << std::endl;
        if (!test->runUpdateTest()) {
            std::cerr << "Failed to run update test for " << test->getName() << std::endl;
        }
        // Run delete test
        std::cout << "  Running delete test..." << std::endl;
        if (!test->runDeleteTest()) {
            std::cerr << "Failed to run delete test for " << test->getName() << std::endl;
        }
        // Run parallel test
        std::cout << "  Running parallel operations test..." << std::endl;
        if (!test->runParallelTest()) {
            std::cerr << "Failed to run parallel test for " << test->getName() << std::endl;
        }

        // Cleanup test
        if (!test->cleanup()) {
            std::cerr << "Failed to clean up " << test->getName() << " test." << std::endl;
        }

        std::cout << "Completed " << test->getName() << " tests." << std::endl;
        std::cout << std::endl;
    }

    // Print results table
    std::cout << "\n===== Benchmark Results =====" << std::endl;

    // Header
    std::cout << std::left << std::setw(20) << "Operation";
    for (const auto& test : tests) {
        std::cout << std::setw(15) << test->getName() + " Time(s)";
        std::cout << std::setw(15) << test->getName() + " Ops";
        std::cout << std::setw(15) << test->getName() + " Ops/s";
    }
    std::cout << std::endl;

    // Insert results
    std::cout << std::left << std::setw(20) << "Insert";
    for (const auto& test : tests) {
        const auto& results = test->results;
        std::cout << std::setw(15) << std::fixed << std::setprecision(3) << results.insertTime;
        std::cout << std::setw(15) << results.insertOps;
        double opsPerSec = results.insertTime > 0 ? results.insertOps / results.insertTime : 0;
        std::cout << std::setw(15) << std::fixed << std::setprecision(1) << opsPerSec;
    }
    std::cout << std::endl;

    // Query results
    std::cout << std::left << std::setw(20) << "Query";
    for (const auto& test : tests) {
        const auto& results = test->results;
        std::cout << std::setw(15) << std::fixed << std::setprecision(3) << results.queryTime;
        std::cout << std::setw(15) << results.queryOps;
        double opsPerSec = results.queryTime > 0 ? results.queryOps / results.queryTime : 0;
        std::cout << std::setw(15) << std::fixed << std::setprecision(1) << opsPerSec;
    }
    std::cout << std::endl;

    // Update results
    std::cout << std::left << std::setw(20) << "Update";
    for (const auto& test : tests) {
        const auto& results = test->results;
        std::cout << std::setw(15) << std::fixed << std::setprecision(3) << results.updateTime;
        std::cout << std::setw(15) << results.updateOps;
        double opsPerSec = results.updateTime > 0 ? results.updateOps / results.updateTime : 0;
        std::cout << std::setw(15) << std::fixed << std::setprecision(1) << opsPerSec;
    }
    std::cout << std::endl;

    // Delete results
    std::cout << std::left << std::setw(20) << "Delete";
    for (const auto& test : tests) {
        const auto& results = test->results;
        std::cout << std::setw(15) << std::fixed << std::setprecision(3) << results.deleteTime;
        std::cout << std::setw(15) << results.deleteOps;
        double opsPerSec = results.deleteTime > 0 ? results.deleteOps / results.deleteTime : 0;
        std::cout << std::setw(15) << std::fixed << std::setprecision(1) << opsPerSec;
    }
    std::cout << std::endl;

    // Parallel results
    std::cout << std::left << std::setw(20) << "Parallel";
    for (const auto& test : tests) {
        const auto& results = test->results;
        std::cout << std::setw(15) << std::fixed << std::setprecision(3) << results.parallelTime;
        std::cout << std::setw(15) << results.parallelOps;
        double opsPerSec = results.parallelTime > 0 ? results.parallelOps / results.parallelTime : 0;
        std::cout << std::setw(15) << std::fixed << std::setprecision(1) << opsPerSec;
    }
    std::cout << std::endl;

    // Generate comparison ratios
    if (tests.size() >= 2) {
        std::cout << "\n===== Performance Comparison =====" << std::endl;
        std::cout << "Ratio of AnuDB to SQLite3 (higher means AnuDB is faster)" << std::endl;

        const auto& anudbResults = tests[0]->results;
        const auto& sqliteResults = tests[1]->results;

        std::cout << std::left << std::setw(20) << "Operation" << std::setw(15) << "Time Ratio" << std::setw(15) << "Ops/s Ratio" << std::endl;

        // Calculate ratios for each operation
        // Insert ratio
        double insertTimeRatio = sqliteResults.insertTime / anudbResults.insertTime;
        double insertOpsRatio = (anudbResults.insertOps / anudbResults.insertTime) / (sqliteResults.insertOps / sqliteResults.insertTime);
        std::cout << std::left << std::setw(20) << "Insert"
                  << std::setw(15) << std::fixed << std::setprecision(2) << insertTimeRatio
                  << std::setw(15) << std::fixed << std::setprecision(2) << insertOpsRatio << std::endl;

        // Query ratio
        double queryTimeRatio = sqliteResults.queryTime / anudbResults.queryTime;
        double queryOpsRatio = (anudbResults.queryOps / anudbResults.queryTime) / (sqliteResults.queryOps / sqliteResults.queryTime);
        std::cout << std::left << std::setw(20) << "Query"
                  << std::setw(15) << std::fixed << std::setprecision(2) << queryTimeRatio
                  << std::setw(15) << std::fixed << std::setprecision(2) << queryOpsRatio << std::endl;

        // Update ratio
        double updateTimeRatio = sqliteResults.updateTime / anudbResults.updateTime;
        double updateOpsRatio = (anudbResults.updateOps / anudbResults.updateTime) / (sqliteResults.updateOps / sqliteResults.updateTime);
        std::cout << std::left << std::setw(20) << "Update"
                  << std::setw(15) << std::fixed << std::setprecision(2) << updateTimeRatio
                  << std::setw(15) << std::fixed << std::setprecision(2) << updateOpsRatio << std::endl;

        // Delete ratio
        double deleteTimeRatio = sqliteResults.deleteTime / anudbResults.deleteTime;
        double deleteOpsRatio = (anudbResults.deleteOps / anudbResults.deleteTime) / (sqliteResults.deleteOps / sqliteResults.deleteTime);
        std::cout << std::left << std::setw(20) << "Delete"
                  << std::setw(15) << std::fixed << std::setprecision(2) << deleteTimeRatio
                  << std::setw(15) << std::fixed << std::setprecision(2) << deleteOpsRatio << std::endl;

        // Parallel ratio
        double parallelTimeRatio = sqliteResults.parallelTime / anudbResults.parallelTime;
        double parallelOpsRatio = (anudbResults.parallelOps / anudbResults.parallelTime) / (sqliteResults.parallelOps / sqliteResults.parallelTime);
        std::cout << std::left << std::setw(20) << "Parallel"
                  << std::setw(15) << std::fixed << std::setprecision(2) << parallelTimeRatio
                  << std::setw(15) << std::fixed << std::setprecision(2) << parallelOpsRatio << std::endl;
    }

    // Generate benchmark report for visualization
    // Write CSV file for easy import into graphing tools
    std::ofstream reportFile("benchmark_results.csv");
    if (reportFile.is_open()) {
        reportFile << "Database,Operation,Time(s),Operations,Ops/s\n";

        for (const auto& test : tests) {
            const auto& results = test->results;
            const std::string& dbName = test->getName();

            // Insert
            reportFile << dbName << ",Insert," << results.insertTime << ","
                       << results.insertOps << "," << (results.insertTime > 0 ? results.insertOps / results.insertTime : 0) << "\n";

            // Query
            reportFile << dbName << ",Query," << results.queryTime << ","
                       << results.queryOps << "," << (results.queryTime > 0 ? results.queryOps / results.queryTime : 0) << "\n";

            // Update
            reportFile << dbName << ",Update," << results.updateTime << ","
                       << results.updateOps << "," << (results.updateTime > 0 ? results.updateOps / results.updateTime : 0) << "\n";

            // Delete
            reportFile << dbName << ",Delete," << results.deleteTime << ","
                       << results.deleteOps << "," << (results.deleteTime > 0 ? results.deleteOps / results.deleteTime : 0) << "\n";

            // Parallel
            reportFile << dbName << ",Parallel," << results.parallelTime << ","
                       << results.parallelOps << "," << (results.parallelTime > 0 ? results.parallelOps / results.parallelTime : 0) << "\n";
        }

        reportFile.close();
        std::cout << "\nBenchmark results saved to 'benchmark_results.csv'" << std::endl;
    }
}

int main() {
    std::cout << "AnuDB Load Testing Benchmark" << std::endl;
    std::cout << "=======================================" << std::endl;

    runBenchmarks();

    return 0;
}
