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

// SQLite3 includes
#include <sqlite3.h>

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
	    std::stringstream ss;
auto now = std::chrono::system_clock::now();
std::time_t now_time_t = std::chrono::system_clock::to_time_t(now);
std::tm* tm_info = std::localtime(&now_time_t);

char time_buffer[20]; // "YYYY-MM-DD HH:MM:SS"
std::strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S", tm_info);
ss << time_buffer;
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

// SQLite3 test implementation
// SQLite3 test implementation
class SQLiteTest : public BenchmarkTest {
public:
    SQLiteTest() : BenchmarkTest("SQLite3") {}

    bool setup() override {
        // Remove existing database file if it exists
        std::remove(DB_PATH_SQLITE.c_str());

        // Open database
        int rc = sqlite3_open(DB_PATH_SQLITE.c_str(), &db);
        if (rc != SQLITE_OK) {
            std::cerr << "Failed to open SQLite database: " << sqlite3_errmsg(db) << std::endl;
            return false;
        }

        // Enable WAL mode for better concurrency
        char *errMsg = nullptr;
        rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            std::cerr << "Failed to set WAL mode: " << errMsg << std::endl;
            sqlite3_free(errMsg);
            return false;
        }

        // Create products table - simplified to match AnuDB's document approach
        const char* createTableSQL =
            "CREATE TABLE products ("
            "id TEXT PRIMARY KEY,"
            "json_data TEXT,"
            "category TEXT,"
            "price REAL,"
            "stock INTEGER,"
            "rating REAL,"
            "available INTEGER"
            ");";

        rc = sqlite3_exec(db, createTableSQL, nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            std::cerr << "Failed to create table: " << errMsg << std::endl;
            sqlite3_free(errMsg);
            return false;
        }

        // Create indexes (same as AnuDB)
        const std::vector<std::string> indexQueries = {
            "CREATE INDEX idx_products_category ON products(category);",
            "CREATE INDEX idx_products_price ON products(price);",
            "CREATE INDEX idx_products_stock ON products(stock);",
            "CREATE INDEX idx_products_rating ON products(rating);",
            "CREATE INDEX idx_products_available ON products(available);"
        };

        for (const auto& query : indexQueries) {
            rc = sqlite3_exec(db, query.c_str(), nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to create index: " << errMsg << std::endl;
                sqlite3_free(errMsg);
                return false;
            }
        }

        // Prepare statements
        const char* insertSQL =
            "INSERT INTO products (id, json_data, category, price, stock, rating, available) "
            "VALUES (?, ?, ?, ?, ?, ?, ?);";
        rc = sqlite3_prepare_v2(db, insertSQL, -1, &insertStmt, nullptr);

        if (rc != SQLITE_OK) {
            std::cerr << "Failed to prepare insert statement: " << sqlite3_errmsg(db) << std::endl;
            return false;
        }

        return true;
    }

    bool cleanup() override {
        if (insertStmt) {
            sqlite3_finalize(insertStmt);
            insertStmt = nullptr;
        }

        if (db) {
            sqlite3_close(db);
            db = nullptr;
        }

        return true;
    }

    bool runInsertTest() override {
        if (!db || !insertStmt) return false;

        int successCount = 0;
        results.insertTime = measureTime([&]() {
            // Begin transaction for bulk insert
            char* errMsg = nullptr;
            int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to begin transaction: " << errMsg << std::endl;
                sqlite3_free(errMsg);
                return;
            }

            for (int i = 0; i < NUM_DOCUMENTS; i++) {
                json productData = generateRandomProduct(i);
                std::string docId = "prod" + std::to_string(i);
                std::string jsonStr = productData.dump();

                // Bind parameters - now focusing on storing the whole JSON document
                sqlite3_reset(insertStmt);
                sqlite3_bind_text(insertStmt, 1, docId.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insertStmt, 2, jsonStr.c_str(), -1, SQLITE_TRANSIENT);
                
                // Extract indexed fields from the JSON to maintain parity with AnuDB's indexing
                sqlite3_bind_text(insertStmt, 3, productData["category"].get<std::string>().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_double(insertStmt, 4, productData["price"].get<double>());
                sqlite3_bind_int(insertStmt, 5, productData["stock"].get<int>());
                sqlite3_bind_double(insertStmt, 6, productData["rating"].get<double>());
                sqlite3_bind_int(insertStmt, 7, productData["available"].get<bool>() ? 1 : 0);

                rc = sqlite3_step(insertStmt);
                if (rc == SQLITE_DONE) {
                    successCount++;
                }
            }

            // Commit transaction
            rc = sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to commit transaction: " << errMsg << std::endl;
                sqlite3_free(errMsg);
            }
        });

        results.insertOps = successCount;
        return true;
    }

    bool runQueryTest() override {
        if (!db) return false;

        int successCount = 0;
        std::vector<std::string> queries = {
            // By category
            "SELECT id, json_data FROM products WHERE category = 'Electronics';",
            "SELECT id, json_data FROM products WHERE category = 'Books';",
            "SELECT id, json_data FROM products WHERE category = 'Food';",
            "SELECT id, json_data FROM products WHERE category = 'Clothing';",

            // By price range
            "SELECT id, json_data FROM products WHERE price > 500.0;",
            "SELECT id, json_data FROM products WHERE price < 100.0;",
            "SELECT id, json_data FROM products WHERE price > 100.0 AND price < 500.0;",

            // By rating
            "SELECT id, json_data FROM products WHERE rating > 4.0;",

            // By availability
            "SELECT id, json_data FROM products WHERE available = 1;",

            // Combined queries
            "SELECT id, json_data FROM products WHERE category = 'Electronics' AND price > 1000.0;"
        };

        results.queryTime = measureTime([&]() {
            for (int i = 0; i < NUM_QUERIES; i++) {
                const std::string& query = queries[i % queries.size()];

                sqlite3_stmt* stmt;
                int rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);

                if (rc == SQLITE_OK) {
                    int count = 0;
                    while (sqlite3_step(stmt) == SQLITE_ROW) {
                        // The document data is in the second column (index 1)
                        const char* jsonData = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                        // Parse the JSON to simulate what AnuDB would do with the returned document
                        //json doc = json::parse(jsonData);
                        count++;
                    }

                    if (count > 0) {
                        successCount++;
                    }
                }

                sqlite3_finalize(stmt);
            }
        });

        results.queryOps = NUM_QUERIES;
        return true;
    }

    bool runUpdateTest() override {
        if (!db) return false;

        int successCount = 0;
        results.updateTime = measureTime([&]() {
            // Begin transaction for bulk update
            char* errMsg = nullptr;
            int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to begin transaction: " << errMsg << std::endl;
                sqlite3_free(errMsg);
                return;
            }

            for (int i = 0; i < NUM_DOCUMENTS / 2; i++) {
                std::string docId = "prod" + std::to_string(i);
                double newPrice = 100.0 + (i % 10) * 50.0;
                int newStock = 10 + (i % 20);

                // First, fetch the existing JSON document
                std::string selectSQL = "SELECT json_data FROM products WHERE id = ?;";
                sqlite3_stmt* selectStmt;
                rc = sqlite3_prepare_v2(db, selectSQL.c_str(), -1, &selectStmt, nullptr);

                if (rc == SQLITE_OK) {
                    sqlite3_bind_text(selectStmt, 1, docId.c_str(), -1, SQLITE_TRANSIENT);

                    if (sqlite3_step(selectStmt) == SQLITE_ROW) {
                        // Get the JSON data
                        const char* jsonStr = reinterpret_cast<const char*>(sqlite3_column_text(selectStmt, 0));
                        json productData = json::parse(jsonStr);

                        // Update the JSON data
                        productData["price"] = newPrice;
                        productData["stock"] = newStock;
                        productData["updated_at"] = getCurrentTimeString();

                        // Now update the database - update both the JSON document and the indexed fields
                        std::string updateSQL =
                            "UPDATE products SET json_data = ?, price = ?, stock = ? WHERE id = ?;";
                        sqlite3_stmt* updateStmt;
                        rc = sqlite3_prepare_v2(db, updateSQL.c_str(), -1, &updateStmt, nullptr);

                        if (rc == SQLITE_OK) {
                            std::string updatedJsonStr = productData.dump();

                            sqlite3_bind_text(updateStmt, 1, updatedJsonStr.c_str(), -1, SQLITE_TRANSIENT);
                            sqlite3_bind_double(updateStmt, 2, newPrice);
                            sqlite3_bind_int(updateStmt, 3, newStock);
                            sqlite3_bind_text(updateStmt, 4, docId.c_str(), -1, SQLITE_TRANSIENT);

                            rc = sqlite3_step(updateStmt);
                            if (rc == SQLITE_DONE) {
                                successCount++;
                            }

                            sqlite3_finalize(updateStmt);
                        }
                    }

                    sqlite3_finalize(selectStmt);
                }
            }

            // Commit transaction
            rc = sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to commit transaction: " << errMsg << std::endl;
                sqlite3_free(errMsg);
            }
        });

        results.updateOps = successCount;
        return true;
    }

    bool runDeleteTest() override {
        if (!db) return false;

        int successCount = 0;
        results.deleteTime = measureTime([&]() {
            // Begin transaction for bulk delete
            char* errMsg = nullptr;
            int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to begin transaction: " << errMsg << std::endl;
                sqlite3_free(errMsg);
                return;
            }

            for (int i = 0; i < NUM_DOCUMENTS / 4; i++) {
                std::string docId = "prod" + std::to_string(i * 3);  // Delete every third document

                std::string deleteSQL = "DELETE FROM products WHERE id = ?;";
                sqlite3_stmt* deleteStmt;
                rc = sqlite3_prepare_v2(db, deleteSQL.c_str(), -1, &deleteStmt, nullptr);

                if (rc == SQLITE_OK) {
                    sqlite3_bind_text(deleteStmt, 1, docId.c_str(), -1, SQLITE_TRANSIENT);

                    rc = sqlite3_step(deleteStmt);
                    if (rc == SQLITE_DONE) {
                        successCount++;
                    }

                    sqlite3_finalize(deleteStmt);
                }
            }

            // Commit transaction
            rc = sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to commit transaction: " << errMsg << std::endl;
                sqlite3_free(errMsg);
            }
        });

        results.deleteOps = successCount;
        return true;
    }

    bool runParallelTest() override {
        if (!db) return false;

        std::vector<std::thread> threads;
        std::atomic<int> successCount(0);
        std::mutex mtx;

        // Create a barrier to synchronize thread start
        std::atomic<int> barrier(0);

        auto start = high_resolution_clock::now();

        // Each thread will need its own connection to the database
        for (int t = 0; t < NUM_THREADS; t++) {
            threads.emplace_back([&, t]() {
                // Wait until all threads are ready
                barrier.fetch_add(1);
                while (barrier.load() < NUM_THREADS) {
                    std::this_thread::yield();
                }

                // Open a new connection for this thread
                sqlite3* threadDb;
                int rc = sqlite3_open(DB_PATH_SQLITE.c_str(), &threadDb);
                if (rc != SQLITE_OK) {
                    std::cerr << "Thread " << t << " failed to open database: " << sqlite3_errmsg(threadDb) << std::endl;
                    return;
                }

                // Create prepared statement for inserts
                sqlite3_stmt* threadInsertStmt;
                const char* insertSQL =
                    "INSERT INTO products (id, json_data, category, price, stock, rating, available) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?);";
                rc = sqlite3_prepare_v2(threadDb, insertSQL, -1, &threadInsertStmt, nullptr);

                if (rc != SQLITE_OK) {
                    std::cerr << "Thread " << t << " failed to prepare insert statement: " << sqlite3_errmsg(threadDb) << std::endl;
                    sqlite3_close(threadDb);
                    return;
                }

                int threadSuccessCount = 0;
                int docsPerThread = NUM_DOCUMENTS / NUM_THREADS;
                int startIdx = t * docsPerThread;
                int endIdx = startIdx + docsPerThread;

                // Begin transaction
                char* errMsg = nullptr;
                rc = sqlite3_exec(threadDb, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg);
                if (rc != SQLITE_OK) {
                    std::cerr << "Thread " << t << " failed to begin transaction: " << errMsg << std::endl;
                    sqlite3_free(errMsg);
                    sqlite3_finalize(threadInsertStmt);
                    sqlite3_close(threadDb);
                    return;
                }

                // Each thread performs a mix of operations
                for (int i = startIdx; i < endIdx; i++) {
                    std::string docId = "parallel_prod" + std::to_string(i);

                    // Insert
                    json productData = generateRandomProduct(i + 10000);  // Offset to avoid conflicts
                    std::string jsonStr = productData.dump();

                    sqlite3_reset(threadInsertStmt);
                    sqlite3_bind_text(threadInsertStmt, 1, docId.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(threadInsertStmt, 2, jsonStr.c_str(), -1, SQLITE_TRANSIENT);
                    // Also maintain indexed fields
                    sqlite3_bind_text(threadInsertStmt, 3, productData["category"].get<std::string>().c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_double(threadInsertStmt, 4, productData["price"].get<double>());
                    sqlite3_bind_int(threadInsertStmt, 5, productData["stock"].get<int>());
                    sqlite3_bind_double(threadInsertStmt, 6, productData["rating"].get<double>());
                    sqlite3_bind_int(threadInsertStmt, 7, productData["available"].get<bool>() ? 1 : 0);

                    rc = sqlite3_step(threadInsertStmt);
                    if (rc == SQLITE_DONE) {
                        threadSuccessCount++;
                    }

                    // Query - using indexed fields when possible
                    if (i % 5 == 0) {
                        std::string category = productData["category"].get<std::string>();
                        std::string querySQL = "SELECT id, json_data FROM products WHERE category = ?;";
                        sqlite3_stmt* queryStmt;
                        rc = sqlite3_prepare_v2(threadDb, querySQL.c_str(), -1, &queryStmt, nullptr);

                        if (rc == SQLITE_OK) {
                            sqlite3_bind_text(queryStmt, 1, category.c_str(), -1, SQLITE_TRANSIENT);

                            while (sqlite3_step(queryStmt) == SQLITE_ROW) {
                                // Parse the returned JSON to simulate AnuDB behavior
                                const char* jsonData = reinterpret_cast<const char*>(sqlite3_column_text(queryStmt, 1));
                                json doc = json::parse(jsonData);
                            }

                            sqlite3_finalize(queryStmt);
                        }
                    }

                    // Update - now updating both JSON document and indexed fields
                    if (i % 3 == 0) {
                        double newPrice = productData["price"].get<double>() * 1.1;
                        int newStock = i % 100;
                        
                        // First get the current JSON document
                        std::string selectSQL = "SELECT json_data FROM products WHERE id = ?;";
                        sqlite3_stmt* selectStmt;
                        rc = sqlite3_prepare_v2(threadDb, selectSQL.c_str(), -1, &selectStmt, nullptr);
                        
                        if (rc == SQLITE_OK) {
                            sqlite3_bind_text(selectStmt, 1, docId.c_str(), -1, SQLITE_TRANSIENT);
                            
                            if (sqlite3_step(selectStmt) == SQLITE_ROW) {
                                // Get and update the JSON document
                                const char* jsonStr = reinterpret_cast<const char*>(sqlite3_column_text(selectStmt, 0));
                                json docData = json::parse(jsonStr);
                                
                                // Modify the document
                                docData["price"] = newPrice;
                                docData["stock"] = newStock;
                                docData["updated_at"] = getCurrentTimeString();
                                
                                // Update the document and indexed fields
                                std::string updateSQL = 
                                    "UPDATE products SET json_data = ?, price = ?, stock = ? WHERE id = ?;";
                                sqlite3_stmt* updateStmt;
                                rc = sqlite3_prepare_v2(threadDb, updateSQL.c_str(), -1, &updateStmt, nullptr);
                                
                                if (rc == SQLITE_OK) {
                                    std::string updatedJsonStr = docData.dump();
                                    
                                    sqlite3_bind_text(updateStmt, 1, updatedJsonStr.c_str(), -1, SQLITE_TRANSIENT);
                                    sqlite3_bind_double(updateStmt, 2, newPrice);
                                    sqlite3_bind_int(updateStmt, 3, newStock);
                                    sqlite3_bind_text(updateStmt, 4, docId.c_str(), -1, SQLITE_TRANSIENT);
                                    
                                    sqlite3_step(updateStmt);
                                    sqlite3_finalize(updateStmt);
                                }
                            }
                            
                            sqlite3_finalize(selectStmt);
                        }
                    }

                    // Delete
                    if (i % 7 == 0) {
                        std::string deleteSQL = "DELETE FROM products WHERE id = ?;";
                        sqlite3_stmt* deleteStmt;
                        rc = sqlite3_prepare_v2(threadDb, deleteSQL.c_str(), -1, &deleteStmt, nullptr);

                        if (rc == SQLITE_OK) {
                            sqlite3_bind_text(deleteStmt, 1, docId.c_str(), -1, SQLITE_TRANSIENT);

                            sqlite3_step(deleteStmt);
                            sqlite3_finalize(deleteStmt);
                        }
                    }
                }

                // Commit transaction
                rc = sqlite3_exec(threadDb, "COMMIT;", nullptr, nullptr, &errMsg);
                if (rc != SQLITE_OK) {
                    std::cerr << "Thread " << t << " failed to commit transaction: " << errMsg << std::endl;
                    sqlite3_free(errMsg);
                }

                // Clean up
                sqlite3_finalize(threadInsertStmt);
                sqlite3_close(threadDb);

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
    sqlite3* db = nullptr;
    sqlite3_stmt* insertStmt = nullptr;
};

// Function to run all tests and print results
void runBenchmarks() {
    std::vector<std::unique_ptr<BenchmarkTest>> tests;
    tests.push_back(std::make_unique<AnuDBTest>());
    tests.push_back(std::make_unique<SQLiteTest>());

    std::cout << "===== Database Benchmark: AnuDB vs SQLITE3  =====" << std::endl;
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
