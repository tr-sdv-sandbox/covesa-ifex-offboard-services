#include "response_consumer.hpp"

#include <glog/logging.h>
#include <librdkafka/rdkafkacpp.h>

#include "rpc_offboard_codec.hpp"

namespace ifex {
namespace cloud {
namespace dispatcher {

class ResponseConsumer::Impl {
public:
    Impl(ResponseConsumer* consumer, ResponseConsumerConfig config)
        : consumer_(consumer), config_(std::move(config)) {}

    ~Impl() {
        stop();
    }

    bool init() {
        std::string errstr;
        auto conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

        conf->set("bootstrap.servers", config_.brokers, errstr);
        conf->set("group.id", config_.group_id, errstr);
        conf->set("client.id", config_.client_id, errstr);
        conf->set("auto.offset.reset", "latest", errstr);
        conf->set("enable.auto.commit", "true", errstr);

        kafka_consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
        if (!kafka_consumer_) {
            LOG(ERROR) << "Failed to create Kafka consumer: " << errstr;
            return false;
        }

        std::vector<std::string> topics = {config_.topic};
        RdKafka::ErrorCode err = kafka_consumer_->subscribe(topics);
        if (err != RdKafka::ERR_NO_ERROR) {
            LOG(ERROR) << "Failed to subscribe to " << config_.topic
                       << ": " << RdKafka::err2str(err);
            return false;
        }

        LOG(INFO) << "Response consumer subscribed to " << config_.topic;
        return true;
    }

    void run() {
        if (!init()) {
            LOG(ERROR) << "Failed to initialize response consumer";
            return;
        }

        LOG(INFO) << "Response consumer thread started";

        while (running_) {
            std::unique_ptr<RdKafka::Message> msg(kafka_consumer_->consume(100));
            if (!msg) continue;

            switch (msg->err()) {
                case RdKafka::ERR_NO_ERROR: {
                    consumer_->stats_.messages_received++;

                    std::string key;
                    if (msg->key()) {
                        key = *msg->key();
                    }
                    std::string value(static_cast<const char*>(msg->payload()),
                                       msg->len());

                    consumer_->process_message(key, value);
                    break;
                }

                case RdKafka::ERR__TIMED_OUT:
                case RdKafka::ERR__PARTITION_EOF:
                    // Normal - no messages available
                    break;

                default:
                    LOG(WARNING) << "Consumer error: " << msg->errstr();
                    break;
            }
        }

        kafka_consumer_->close();
        LOG(INFO) << "Response consumer thread stopped";
    }

    void stop() {
        running_ = false;
    }

    std::atomic<bool> running_{false};

private:
    ResponseConsumer* consumer_;
    ResponseConsumerConfig config_;
    std::unique_ptr<RdKafka::KafkaConsumer> kafka_consumer_;
};

ResponseConsumer::ResponseConsumer(ResponseConsumerConfig config,
                                   std::shared_ptr<RpcRequestManager> request_manager)
    : config_(std::move(config)),
      request_manager_(std::move(request_manager)),
      impl_(std::make_unique<Impl>(this, config_)) {}

ResponseConsumer::~ResponseConsumer() {
    stop();
}

void ResponseConsumer::start() {
    if (running_) {
        LOG(WARNING) << "Response consumer already running";
        return;
    }

    running_ = true;
    impl_->running_ = true;
    consumer_thread_ = std::thread([this]() { impl_->run(); });

    LOG(INFO) << "Response consumer started";
}

void ResponseConsumer::stop() {
    if (!running_) return;

    running_ = false;
    impl_->stop();

    if (consumer_thread_.joinable()) {
        consumer_thread_.join();
    }

    LOG(INFO) << "Response consumer stopped";
}

void ResponseConsumer::process_message(const std::string& key, const std::string& value) {
    // Decode the offboard RPC message
    auto msg = ifex::offboard::decode_rpc_offboard(value);
    if (!msg) {
        LOG(WARNING) << "Failed to decode RPC offboard message";
        stats_.parse_errors++;
        return;
    }

    // Check if it's a response
    if (!msg->has_response()) {
        VLOG(1) << "Received non-response RPC message, ignoring";
        return;
    }

    const auto& response = msg->response();

    // Build RpcResponse
    RpcResponse rpc_response;
    rpc_response.correlation_id = response.correlation_id();
    rpc_response.vehicle_id = msg->metadata().vehicle_id();

    // Map proto status to our status
    switch (response.status()) {
        case swdv::dispatcher_rpc_envelope::SUCCESS:
            rpc_response.status = RpcStatus::SUCCESS;
            break;
        case swdv::dispatcher_rpc_envelope::TIMEOUT:
            rpc_response.status = RpcStatus::TIMEOUT;
            break;
        case swdv::dispatcher_rpc_envelope::FAILED:
        case swdv::dispatcher_rpc_envelope::SERVICE_UNAVAILABLE:
        case swdv::dispatcher_rpc_envelope::METHOD_NOT_FOUND:
        case swdv::dispatcher_rpc_envelope::INVALID_PARAMETERS:
        case swdv::dispatcher_rpc_envelope::TRANSPORT_ERROR:
        case swdv::dispatcher_rpc_envelope::DUPLICATE_REQUEST:
        default:
            rpc_response.status = RpcStatus::ERROR;
            break;
    }

    rpc_response.result_json = response.result_json();
    rpc_response.error_message = response.error_message();
    rpc_response.execution_time_ms = response.duration_ms();

    // Dispatch to request manager
    request_manager_->on_response(rpc_response);
    stats_.responses_processed++;

    LOG(INFO) << "Processed response: correlation_id=" << rpc_response.correlation_id
              << " status=" << rpc_status_to_string(rpc_response.status);
}

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
