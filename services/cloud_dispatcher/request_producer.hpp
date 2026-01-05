#pragma once

#include <atomic>
#include <memory>
#include <string>

namespace ifex {
namespace cloud {
namespace dispatcher {

/// Configuration for request producer
struct RequestProducerConfig {
    std::string brokers = "localhost:9092";
    std::string topic = "ifex.c2v.rpc";
    std::string client_id = "cloud-dispatcher-producer";
};

/// Statistics for request producer
struct RequestProducerStats {
    std::atomic<uint64_t> messages_sent{0};
    std::atomic<uint64_t> send_errors{0};
};

/**
 * Kafka producer for RPC requests
 *
 * Produces to ifex.c2v.rpc with vehicle_id as key
 */
class RequestProducer {
public:
    explicit RequestProducer(RequestProducerConfig config);
    ~RequestProducer();

    /// Initialize the producer
    bool init();

    /// Send an RPC request
    /// @param vehicle_id Target vehicle (used as Kafka key for partitioning)
    /// @param payload Serialized RPC request
    bool send(const std::string& vehicle_id, const std::string& payload);

    /// Flush pending messages
    void flush(int timeout_ms = 5000);

    /// Get statistics
    const RequestProducerStats& stats() const { return stats_; }

private:
    RequestProducerConfig config_;
    RequestProducerStats stats_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
