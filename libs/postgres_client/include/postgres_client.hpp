#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <libpq-fe.h>

namespace ifex::offboard {

/// Configuration for PostgreSQL connection
struct PostgresConfig {
    std::string host = "localhost";
    int port = 5432;
    std::string database = "ifex_offboard";
    std::string user = "ifex";
    std::string password = "ifex_dev";
    int connect_timeout = 10;
};

/// Row from a query result
class PostgresRow {
public:
    PostgresRow(PGresult* result, int row);

    /// Get column value as string (empty string if NULL)
    std::string get_string(int col) const;
    std::string get_string(const std::string& col_name) const;

    /// Get column value as int (0 if NULL)
    int get_int(int col) const;
    int get_int(const std::string& col_name) const;

    /// Get column value as int64 (0 if NULL)
    int64_t get_int64(int col) const;
    int64_t get_int64(const std::string& col_name) const;

    /// Check if column is NULL
    bool is_null(int col) const;
    bool is_null(const std::string& col_name) const;

    /// Get number of columns
    int num_columns() const;

private:
    int get_col_index(const std::string& col_name) const;

    PGresult* result_;
    int row_;
};

/// Query result wrapper
class PostgresResult {
public:
    explicit PostgresResult(PGresult* result);
    ~PostgresResult();

    PostgresResult(PostgresResult&& other) noexcept;
    PostgresResult& operator=(PostgresResult&& other) noexcept;

    // Non-copyable
    PostgresResult(const PostgresResult&) = delete;
    PostgresResult& operator=(const PostgresResult&) = delete;

    /// Check if query was successful
    bool ok() const;

    /// Get error message (empty if ok)
    std::string error() const;

    /// Get number of rows
    int num_rows() const;

    /// Get number of columns
    int num_columns() const;

    /// Get column name
    std::string column_name(int col) const;

    /// Get a row
    PostgresRow row(int index) const;

    /// Get number of affected rows (for INSERT/UPDATE/DELETE)
    int affected_rows() const;

    /// Iterate over rows
    class Iterator {
    public:
        Iterator(const PostgresResult* result, int row);
        PostgresRow operator*() const;
        Iterator& operator++();
        bool operator!=(const Iterator& other) const;

    private:
        const PostgresResult* result_;
        int row_;
    };

    Iterator begin() const;
    Iterator end() const;

private:
    PGresult* result_;
};

/// PostgreSQL client wrapper
class PostgresClient {
public:
    explicit PostgresClient(const PostgresConfig& config);
    ~PostgresClient();

    // Non-copyable
    PostgresClient(const PostgresClient&) = delete;
    PostgresClient& operator=(const PostgresClient&) = delete;

    /// Check if connected
    bool is_connected() const;

    /// Reconnect if connection was lost
    bool reconnect();

    /// Execute a query with no parameters
    PostgresResult execute(const std::string& query);

    /// Execute a query with parameters (prevents SQL injection)
    PostgresResult execute(const std::string& query,
                          const std::vector<std::string>& params);

    /// Execute and return single value (for COUNT, etc.)
    std::optional<std::string> execute_scalar(const std::string& query);
    std::optional<std::string> execute_scalar(
        const std::string& query,
        const std::vector<std::string>& params);

    /// Begin a transaction
    bool begin_transaction();

    /// Commit transaction
    bool commit();

    /// Rollback transaction
    bool rollback();

    /// Escape a string for use in queries (prefer parameterized queries)
    std::string escape_string(const std::string& str);

    /// Get last error message
    std::string last_error() const;

private:
    void connect();

    PostgresConfig config_;
    PGconn* conn_ = nullptr;
};

}  // namespace ifex::offboard
