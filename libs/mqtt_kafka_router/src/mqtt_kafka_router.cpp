#include "mqtt_kafka_router.hpp"

#include <glog/logging.h>
#include <librdkafka/rdkafkacpp.h>
#include <mqtt/async_client.h>

#include <regex>
#include <sstream>

namespace ifex::offboard {

/// Implementation details using pimpl pattern
class MqttKafkaRouter::Impl : public virtual mqtt::callback {
public:
    Impl(MqttKafkaRouter* router, MqttRouterConfig mqtt_config,
         KafkaRouterConfig kafka_config)
        : router_(router),
          mqtt_config_(std::move(mqtt_config)),
          kafka_config_(std::move(kafka_config)) {}

    ~Impl() { stop(); }

    bool init() {
        // Initialize Kafka producer
        std::string errstr;
        auto kafka_conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

        kafka_conf->set("bootstrap.servers", kafka_config_.brokers, errstr);
        kafka_conf->set("client.id", kafka_config_.client_id, errstr);
        kafka_conf->set("queue.buffering.max.ms",
                        std::to_string(kafka_config_.queue_buffering_max_ms), errstr);
        kafka_conf->set("batch.num.messages",
                        std::to_string(kafka_config_.batch_num_messages), errstr);

        // Delivery report callback
        kafka_conf->set("dr_cb", &delivery_cb_, errstr);

        producer_.reset(RdKafka::Producer::create(kafka_conf.get(), errstr));
        if (!producer_) {
            LOG(ERROR) << "Failed to create Kafka producer: " << errstr;
            return false;
        }

        LOG(INFO) << "Kafka producer initialized: " << kafka_config_.brokers;

        // Initialize MQTT client
        std::string mqtt_uri =
            "tcp://" + mqtt_config_.host + ":" + std::to_string(mqtt_config_.port);

        mqtt_client_ = std::make_unique<mqtt::async_client>(mqtt_uri,
                                                             mqtt_config_.client_id);

        // Set callbacks
        mqtt_client_->set_callback(*this);

        LOG(INFO) << "MQTT client initialized: " << mqtt_uri;
        return true;
    }

    void run() {
        if (!init()) {
            LOG(FATAL) << "Failed to initialize router";
            return;
        }

        // Connect to MQTT
        mqtt::connect_options conn_opts;
        conn_opts.set_clean_session(true);
        conn_opts.set_automatic_reconnect(true);
        conn_opts.set_connect_timeout(std::chrono::seconds(10));

        if (!mqtt_config_.username.empty()) {
            conn_opts.set_user_name(mqtt_config_.username);
            conn_opts.set_password(mqtt_config_.password);
        }

        try {
            LOG(INFO) << "Connecting to MQTT broker...";
            mqtt_client_->connect(conn_opts)->wait();
            LOG(INFO) << "Connected to MQTT broker";

            LOG(INFO) << "Subscribing to: " << mqtt_config_.topic_pattern;
            mqtt_client_->subscribe(mqtt_config_.topic_pattern, 1)->wait();
            LOG(INFO) << "Subscribed to: " << mqtt_config_.topic_pattern;

        } catch (const mqtt::exception& e) {
            LOG(FATAL) << "MQTT connection failed: " << e.what();
            return;
        }

        running_ = true;
        LOG(INFO) << "Router running, press Ctrl+C to stop";

        // Main loop - poll Kafka for delivery reports
        while (running_) {
            producer_->poll(100);
        }

        // Cleanup
        LOG(INFO) << "Shutting down...";
        mqtt_client_->disconnect()->wait();
        producer_->flush(5000);
    }

    void stop() {
        running_ = false;
    }

    // MQTT callback interface
    void connected(const std::string& cause) {
        LOG(INFO) << "MQTT connected: " << cause;
        // Resubscribe on reconnect
        mqtt_client_->subscribe(mqtt_config_.topic_pattern, 1);
    }

    void connection_lost(const std::string& cause) {
        LOG(WARNING) << "MQTT connection lost: " << cause;
    }

    void message_arrived(mqtt::const_message_ptr msg) {
        router_->on_message(msg->get_topic(), msg->get_payload_str());
    }

    void delivery_complete(mqtt::delivery_token_ptr) {}

    void produce(const std::string& topic, const std::string& key,
                 const std::string& value) {
        RdKafka::ErrorCode err = producer_->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,  // auto partition
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(value.data()), value.size(),
            key.data(), key.size(),
            0,       // timestamp (0 = now)
            nullptr  // opaque
        );

        if (err != RdKafka::ERR_NO_ERROR) {
            LOG(ERROR) << "Failed to produce to " << topic << ": "
                       << RdKafka::err2str(err);
            router_->stats_.produce_errors++;
        } else {
            router_->stats_.messages_produced++;
        }
    }

private:
    // Kafka delivery report callback
    class DeliveryCb : public RdKafka::DeliveryReportCb {
    public:
        void dr_cb(RdKafka::Message& message) override {
            if (message.err()) {
                LOG(ERROR) << "Delivery failed: " << message.errstr();
            } else {
                VLOG(2) << "Delivered to " << message.topic_name()
                        << " [" << message.partition() << "]"
                        << " offset " << message.offset();
            }
        }
    };

    MqttKafkaRouter* router_;
    MqttRouterConfig mqtt_config_;
    KafkaRouterConfig kafka_config_;

    std::unique_ptr<mqtt::async_client> mqtt_client_;
    std::unique_ptr<RdKafka::Producer> producer_;
    DeliveryCb delivery_cb_;

    std::atomic<bool> running_{false};
};

// MqttKafkaRouter implementation

MqttKafkaRouter::MqttKafkaRouter(MqttRouterConfig mqtt_config,
                                   KafkaRouterConfig kafka_config)
    : mqtt_config_(std::move(mqtt_config)),
      kafka_config_(std::move(kafka_config)),
      impl_(std::make_unique<Impl>(this, mqtt_config_, kafka_config_)) {}

MqttKafkaRouter::~MqttKafkaRouter() = default;

void MqttKafkaRouter::register_handler(uint32_t content_id,
                                        const std::string& kafka_topic,
                                        TransformFn transform) {
    handlers_[content_id] = ContentHandler{content_id, kafka_topic, std::move(transform)};
    LOG(INFO) << "Registered handler for content_id=" << content_id
              << " -> " << kafka_topic;
}

void MqttKafkaRouter::set_context_store(std::shared_ptr<VehicleContextStore> store) {
    context_store_ = std::move(store);
}

void MqttKafkaRouter::set_require_context(bool require) {
    require_context_ = require;
}

void MqttKafkaRouter::run() {
    running_ = true;
    impl_->run();
}

void MqttKafkaRouter::stop() {
    running_ = false;
    impl_->stop();
}

std::optional<TopicInfo> MqttKafkaRouter::parse_topic(const std::string& topic,
                                                       const std::string& prefix) {
    // Expected format: {prefix}{vehicle_id}/{content_id}
    // e.g., "v2c/vehicle-001/201"

    if (topic.size() <= prefix.size() || topic.substr(0, prefix.size()) != prefix) {
        return std::nullopt;
    }

    std::string remainder = topic.substr(prefix.size());

    // Find the last '/' to split vehicle_id and content_id
    size_t last_slash = remainder.rfind('/');
    if (last_slash == std::string::npos || last_slash == 0 ||
        last_slash == remainder.size() - 1) {
        return std::nullopt;
    }

    TopicInfo info;
    info.vehicle_id = remainder.substr(0, last_slash);

    try {
        info.content_id = std::stoul(remainder.substr(last_slash + 1));
    } catch (const std::exception&) {
        return std::nullopt;
    }

    return info;
}

void MqttKafkaRouter::on_message(const std::string& topic, const std::string& payload) {
    stats_.messages_received++;

    auto info = parse_topic(topic, mqtt_config_.topic_prefix);
    if (!info) {
        LOG(WARNING) << "Failed to parse topic: " << topic;
        return;
    }

    VLOG(1) << "Received: vehicle=" << info->vehicle_id
            << " content_id=" << info->content_id
            << " size=" << payload.size();

    process_message(*info, payload);
}

void MqttKafkaRouter::process_message(const TopicInfo& info,
                                       const std::string& payload) {
    // Find handler for this content_id
    auto it = handlers_.find(info.content_id);
    if (it == handlers_.end()) {
        VLOG(1) << "No handler for content_id=" << info.content_id;
        stats_.unknown_content_ids++;
        return;
    }

    const auto& handler = it->second;

    // Get vehicle context (if store is configured)
    std::optional<VehicleContext> ctx;
    if (context_store_) {
        ctx = context_store_->get(info.vehicle_id);
        if (!ctx && require_context_) {
            VLOG(1) << "Unknown vehicle (context required): " << info.vehicle_id;
            stats_.unknown_vehicles++;
            return;
        }
    }

    // Transform the message
    std::optional<std::string> transformed;
    try {
        transformed = handler.transform(info.vehicle_id, info.content_id, payload, ctx);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Transform error for vehicle=" << info.vehicle_id
                   << " content_id=" << info.content_id << ": " << e.what();
        stats_.transform_errors++;
        return;
    }

    if (!transformed) {
        VLOG(2) << "Message dropped by transform";
        stats_.messages_dropped++;
        return;
    }

    stats_.messages_transformed++;

    // Produce to Kafka (vehicle_id as key for partitioning)
    impl_->produce(handler.kafka_topic, info.vehicle_id, *transformed);
}

}  // namespace ifex::offboard
