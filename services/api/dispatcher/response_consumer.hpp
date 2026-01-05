#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "rpc_request_manager.hpp"

namespace ifex {
namespace cloud {
namespace dispatcher {

/// Configuration for response consumer
struct ResponseConsumerConfig {
    std::string brokers = "localhost:9092";
    std::string topic = "ifex.rpc.200";
    std::string group_id = "cloud-dispatcher";
    std::string client_id = "cloud-dispatcher-consumer";
};

/// Statistics for response consumer
struct ResponseConsumerStats {
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> responses_processed{0};
    std::atomic<uint64_t> parse_errors{0};
};

/**
 * Background Kafka consumer for RPC responses
 *
 * Consumes from ifex.rpc.200 and routes responses to RpcRequestManager
 */
class ResponseConsumer {
public:
    ResponseConsumer(ResponseConsumerConfig config,
                     std::shared_ptr<RpcRequestManager> request_manager);
    ~ResponseConsumer();

    /// Start consuming in background thread
    void start();

    /// Stop consuming
    void stop();

    /// Get statistics
    const ResponseConsumerStats& stats() const { return stats_; }

private:
    void run();
    void process_message(const std::string& key, const std::string& value);

    ResponseConsumerConfig config_;
    std::shared_ptr<RpcRequestManager> request_manager_;

    std::atomic<bool> running_{false};
    std::thread consumer_thread_;
    ResponseConsumerStats stats_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
