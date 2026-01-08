#include "postgres_client.hpp"

#include <sstream>

#include <glog/logging.h>

namespace ifex::offboard {

// PostgresRow implementation
PostgresRow::PostgresRow(PGresult* result, int row)
    : result_(result), row_(row) {}

std::string PostgresRow::get_string(int col) const {
    if (PQgetisnull(result_, row_, col)) {
        return "";
    }
    return PQgetvalue(result_, row_, col);
}

std::string PostgresRow::get_string(const std::string& col_name) const {
    return get_string(get_col_index(col_name));
}

int PostgresRow::get_int(int col) const {
    return std::stoi(PQgetvalue(result_, row_, col));
}

int PostgresRow::get_int(const std::string& col_name) const {
    return get_int(get_col_index(col_name));
}

int64_t PostgresRow::get_int64(int col) const {
    return std::stoll(PQgetvalue(result_, row_, col));
}

int64_t PostgresRow::get_int64(const std::string& col_name) const {
    return get_int64(get_col_index(col_name));
}

bool PostgresRow::is_null(int col) const {
    return PQgetisnull(result_, row_, col) == 1;
}

bool PostgresRow::is_null(const std::string& col_name) const {
    return is_null(get_col_index(col_name));
}

int PostgresRow::num_columns() const {
    return PQnfields(result_);
}

int PostgresRow::get_col_index(const std::string& col_name) const {
    int col = PQfnumber(result_, col_name.c_str());
    if (col < 0) {
        LOG(WARNING) << "Column not found: " << col_name;
    }
    return col;
}

// PostgresResult implementation
PostgresResult::PostgresResult(PGresult* result) : result_(result) {}

PostgresResult::~PostgresResult() {
    if (result_) {
        PQclear(result_);
    }
}

PostgresResult::PostgresResult(PostgresResult&& other) noexcept
    : result_(other.result_) {
    other.result_ = nullptr;
}

PostgresResult& PostgresResult::operator=(PostgresResult&& other) noexcept {
    if (this != &other) {
        if (result_) {
            PQclear(result_);
        }
        result_ = other.result_;
        other.result_ = nullptr;
    }
    return *this;
}

bool PostgresResult::ok() const {
    if (!result_) return false;
    ExecStatusType status = PQresultStatus(result_);
    return status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK;
}

std::string PostgresResult::error() const {
    if (!result_) return "No result";
    return PQresultErrorMessage(result_);
}

int PostgresResult::num_rows() const {
    return result_ ? PQntuples(result_) : 0;
}

int PostgresResult::num_columns() const {
    return result_ ? PQnfields(result_) : 0;
}

std::string PostgresResult::column_name(int col) const {
    return result_ ? PQfname(result_, col) : "";
}

PostgresRow PostgresResult::row(int index) const {
    return PostgresRow(result_, index);
}

int PostgresResult::affected_rows() const {
    if (!result_) return 0;
    const char* val = PQcmdTuples(result_);
    if (!val || val[0] == '\0') return 0;
    return std::stoi(val);
}

PostgresResult::Iterator::Iterator(const PostgresResult* result, int row)
    : result_(result), row_(row) {}

PostgresRow PostgresResult::Iterator::operator*() const {
    return result_->row(row_);
}

PostgresResult::Iterator& PostgresResult::Iterator::operator++() {
    ++row_;
    return *this;
}

bool PostgresResult::Iterator::operator!=(const Iterator& other) const {
    return row_ != other.row_;
}

PostgresResult::Iterator PostgresResult::begin() const {
    return Iterator(this, 0);
}

PostgresResult::Iterator PostgresResult::end() const {
    return Iterator(this, num_rows());
}

// PostgresClient implementation
PostgresClient::PostgresClient(const PostgresConfig& config)
    : config_(config) {
    connect();
}

PostgresClient::~PostgresClient() {
    if (conn_) {
        PQfinish(conn_);
    }
}

void PostgresClient::connect() {
    std::ostringstream conn_str;
    conn_str << "host=" << config_.host
             << " port=" << config_.port
             << " dbname=" << config_.database
             << " user=" << config_.user
             << " password=" << config_.password
             << " connect_timeout=" << config_.connect_timeout;

    conn_ = PQconnectdb(conn_str.str().c_str());

    if (PQstatus(conn_) != CONNECTION_OK) {
        LOG(ERROR) << "PostgreSQL connection failed: " << PQerrorMessage(conn_);
        PQfinish(conn_);
        conn_ = nullptr;
    } else {
        LOG(INFO) << "Connected to PostgreSQL: " << config_.database
                  << "@" << config_.host << ":" << config_.port;
    }
}

bool PostgresClient::is_connected() const {
    return conn_ && PQstatus(conn_) == CONNECTION_OK;
}

bool PostgresClient::reconnect() {
    if (conn_) {
        PQfinish(conn_);
        conn_ = nullptr;
    }
    connect();
    return is_connected();
}

PostgresResult PostgresClient::execute(const std::string& query) {
    if (!is_connected()) {
        LOG(WARNING) << "Not connected, attempting reconnect";
        if (!reconnect()) {
            return PostgresResult(nullptr);
        }
    }

    PGresult* result = PQexec(conn_, query.c_str());
    PostgresResult res(result);

    if (!res.ok()) {
        LOG(ERROR) << "Query failed: " << res.error();
    }

    return res;
}

PostgresResult PostgresClient::execute(const std::string& query,
                                       const std::vector<std::string>& params) {
    if (!is_connected()) {
        LOG(WARNING) << "Not connected, attempting reconnect";
        if (!reconnect()) {
            return PostgresResult(nullptr);
        }
    }

    std::vector<const char*> param_values;
    param_values.reserve(params.size());
    for (const auto& p : params) {
        param_values.push_back(p.c_str());
    }

    PGresult* result = PQexecParams(
        conn_,
        query.c_str(),
        static_cast<int>(params.size()),
        nullptr,  // Let PostgreSQL infer types
        param_values.data(),
        nullptr,  // Text format for all params
        nullptr,  // Text format for all params
        0         // Text format for results
    );

    PostgresResult res(result);

    if (!res.ok()) {
        LOG(ERROR) << "Query failed: " << res.error();
    }

    return res;
}

std::optional<std::string> PostgresClient::execute_scalar(
    const std::string& query) {
    auto result = execute(query);
    if (result.ok() && result.num_rows() > 0) {
        return result.row(0).get_string(0);
    }
    return std::nullopt;
}

std::optional<std::string> PostgresClient::execute_scalar(
    const std::string& query,
    const std::vector<std::string>& params) {
    auto result = execute(query, params);
    if (result.ok() && result.num_rows() > 0) {
        return result.row(0).get_string(0);
    }
    return std::nullopt;
}

bool PostgresClient::begin_transaction() {
    auto result = execute("BEGIN");
    return result.ok();
}

bool PostgresClient::commit() {
    auto result = execute("COMMIT");
    return result.ok();
}

bool PostgresClient::rollback() {
    auto result = execute("ROLLBACK");
    return result.ok();
}

std::string PostgresClient::escape_string(const std::string& str) {
    if (!conn_) return str;

    char* escaped = PQescapeLiteral(conn_, str.c_str(), str.size());
    if (!escaped) {
        LOG(ERROR) << "Failed to escape string: " << PQerrorMessage(conn_);
        return str;
    }

    std::string result(escaped);
    PQfreemem(escaped);
    return result;
}

std::string PostgresClient::last_error() const {
    return conn_ ? PQerrorMessage(conn_) : "Not connected";
}

}  // namespace ifex::offboard
