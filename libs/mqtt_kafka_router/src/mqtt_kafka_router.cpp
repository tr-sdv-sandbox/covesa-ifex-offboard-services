#include "mqtt_kafka_router.hpp"

#include <glog/logging.h>
#include <librdkafka/rdkafkacpp.h>
#include <mqtt/async_client.h>

#include <regex>
#include <sstream>
#include <thread>

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
        // Initialize Kafka producer (for v2c)
        std::string errstr;
        auto producer_conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

        producer_conf->set("bootstrap.servers", kafka_config_.brokers, errstr);
        producer_conf->set("client.id", kafka_config_.client_id + "-producer", errstr);
        producer_conf->set("queue.buffering.max.ms",
                        std::to_string(kafka_config_.queue_buffering_max_ms), errstr);
        producer_conf->set("batch.num.messages",
                        std::to_string(kafka_config_.batch_num_messages), errstr);

        // Delivery report callback
        producer_conf->set("dr_cb", &delivery_cb_, errstr);

        producer_.reset(RdKafka::Producer::create(producer_conf.get(), errstr));
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

    bool init_c2v_consumer(const std::vector<std::string>& topics) {
        if (topics.empty()) {
            LOG(INFO) << "No c2v topics configured, skipping consumer init";
            return true;
        }

        std::string errstr;
        auto consumer_conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

        consumer_conf->set("bootstrap.servers", kafka_config_.brokers, errstr);
        consumer_conf->set("client.id", kafka_config_.client_id + "-c2v-consumer", errstr);
        consumer_conf->set("group.id", kafka_config_.c2v_group_id, errstr);
        consumer_conf->set("auto.offset.reset", "latest", errstr);
        consumer_conf->set("enable.auto.commit", "true", errstr);

        consumer_.reset(RdKafka::KafkaConsumer::create(consumer_conf.get(), errstr));
        if (!consumer_) {
            LOG(ERROR) << "Failed to create Kafka consumer: " << errstr;
            return false;
        }

        RdKafka::ErrorCode err = consumer_->subscribe(topics);
        if (err != RdKafka::ERR_NO_ERROR) {
            LOG(ERROR) << "Failed to subscribe to c2v topics: " << RdKafka::err2str(err);
            return false;
        }

        LOG(INFO) << "Kafka c2v consumer initialized, subscribed to " << topics.size() << " topics";
        for (const auto& topic : topics) {
            LOG(INFO) << "  - " << topic;
        }

        return true;
    }

    void run(const std::vector<std::string>& c2v_topics) {
        if (!init()) {
            LOG(FATAL) << "Failed to initialize router";
            return;
        }

        // Initialize c2v consumer if there are topics
        if (!init_c2v_consumer(c2v_topics)) {
            LOG(ERROR) << "Failed to initialize c2v consumer";
            // Continue without c2v - v2c will still work
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

        // Start c2v consumer thread if consumer exists
        std::thread c2v_thread;
        if (consumer_) {
            c2v_thread = std::thread([this]() { run_c2v_consumer(); });
        }

        // Main loop - poll Kafka producer for delivery reports
        while (running_) {
            producer_->poll(100);
        }

        // Cleanup
        LOG(INFO) << "Shutting down...";

        // Stop c2v consumer
        if (consumer_) {
            consumer_->close();
        }
        if (c2v_thread.joinable()) {
            c2v_thread.join();
        }

        mqtt_client_->disconnect()->wait();
        producer_->flush(5000);
    }

    void run_c2v_consumer() {
        LOG(INFO) << "c2v consumer thread started";

        while (running_) {
            std::unique_ptr<RdKafka::Message> msg(consumer_->consume(100));
            if (!msg) continue;

            switch (msg->err()) {
                case RdKafka::ERR_NO_ERROR: {
                    // Extract key and value
                    std::string key;
                    if (msg->key()) {
                        key = *msg->key();
                    }
                    std::string value(static_cast<const char*>(msg->payload()),
                                       msg->len());
                    std::string topic = msg->topic_name();

                    VLOG(1) << "c2v message: topic=" << topic
                            << " key=" << key << " size=" << value.size();

                    router_->process_c2v_message(topic, key, value);
                    break;
                }

                case RdKafka::ERR__TIMED_OUT:
                case RdKafka::ERR__PARTITION_EOF:
                    // Normal - no messages available
                    break;

                default:
                    LOG(WARNING) << "c2v consumer error: " << msg->errstr();
                    break;
            }
        }

        LOG(INFO) << "c2v consumer thread stopped";
    }

    void stop() {
        running_ = false;
    }

    // MQTT callback interface
    void connected(const std::string& cause) override {
        LOG(INFO) << "MQTT connected: " << cause;
        // Resubscribe on reconnect
        mqtt_client_->subscribe(mqtt_config_.topic_pattern, 1);
    }

    void connection_lost(const std::string& cause) override {
        LOG(WARNING) << "MQTT connection lost: " << cause;
    }

    void message_arrived(mqtt::const_message_ptr msg) override {
        router_->on_message(msg->get_topic(), msg->get_payload_str());
    }

    void delivery_complete(mqtt::delivery_token_ptr) override {}

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

    void publish_mqtt(const std::string& topic, const std::string& payload) {
        try {
            auto msg = mqtt::make_message(topic, payload);
            msg->set_qos(1);
            mqtt_client_->publish(msg)->wait_for(std::chrono::seconds(5));
            router_->stats_.c2v_messages_published++;

            VLOG(1) << "Published to MQTT: " << topic << " (" << payload.size() << " bytes)";

        } catch (const mqtt::exception& e) {
            LOG(ERROR) << "Failed to publish to MQTT " << topic << ": " << e.what();
            router_->stats_.c2v_publish_errors++;
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
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;  // for c2v
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
    LOG(INFO) << "Registered v2c handler for content_id=" << content_id
              << " -> " << kafka_topic;
}

void MqttKafkaRouter::register_c2v_handler(const std::string& kafka_topic,
                                            uint32_t content_id,
                                            C2vTransformFn transform) {
    c2v_handlers_[kafka_topic] = C2vHandler{kafka_topic, content_id, std::move(transform)};
    LOG(INFO) << "Registered c2v handler for " << kafka_topic
              << " -> content_id=" << content_id;
}

void MqttKafkaRouter::set_context_store(std::shared_ptr<VehicleContextStore> store) {
    context_store_ = std::move(store);
}

void MqttKafkaRouter::set_require_context(bool require) {
    require_context_ = require;
}

void MqttKafkaRouter::set_status_handler(const std::string& kafka_topic,
                                          StatusCallback cb) {
    status_kafka_topic_ = kafka_topic;
    status_callback_ = std::move(cb);
    LOG(INFO) << "Registered status handler -> " << kafka_topic;
}

std::string MqttKafkaRouter::build_c2v_topic(const std::string& vehicle_id,
                                              uint32_t content_id) const {
    return mqtt_config_.c2v_topic_prefix + vehicle_id + "/" + std::to_string(content_id);
}

void MqttKafkaRouter::run() {
    running_ = true;

    // Collect c2v topics for consumer subscription
    std::vector<std::string> c2v_topics;
    for (const auto& [topic, handler] : c2v_handlers_) {
        c2v_topics.push_back(topic);
    }

    impl_->run(c2v_topics);
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

    // Check for is_online status topic: v2c/{vehicle_id}/is_online
    static const std::string is_online_suffix = "/is_online";
    if (topic.size() > mqtt_config_.topic_prefix.size() + is_online_suffix.size() &&
        topic.compare(topic.size() - is_online_suffix.size(), is_online_suffix.size(), is_online_suffix) == 0) {

        // Extract vehicle_id from topic: v2c/{vehicle_id}/is_online
        size_t prefix_len = mqtt_config_.topic_prefix.size();
        size_t suffix_len = is_online_suffix.size();
        std::string vehicle_id = topic.substr(prefix_len, topic.size() - prefix_len - suffix_len);

        bool is_online = (payload == "1");

        VLOG(1) << "Status: vehicle=" << vehicle_id << " is_online=" << is_online;
        stats_.status_messages_received++;

        // Produce to Kafka status topic if configured
        if (!status_kafka_topic_.empty()) {
            // Payload format: vehicle_id:0 or vehicle_id:1
            std::string kafka_payload = vehicle_id + ":" + payload;
            impl_->produce(status_kafka_topic_, vehicle_id, kafka_payload);
            stats_.status_messages_produced++;
        }

        // Call status callback for DB update
        if (status_callback_) {
            status_callback_(vehicle_id, is_online);
        }

        return;
    }

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

void MqttKafkaRouter::process_c2v_message(const std::string& kafka_topic,
                                           const std::string& key,
                                           const std::string& value) {
    stats_.c2v_messages_received++;

    // Find handler for this Kafka topic
    auto it = c2v_handlers_.find(kafka_topic);
    if (it == c2v_handlers_.end()) {
        LOG(WARNING) << "No c2v handler for topic: " << kafka_topic;
        return;
    }

    const auto& handler = it->second;

    // Transform or use default (key = vehicle_id, value = payload)
    std::string vehicle_id;
    std::string payload;

    if (handler.transform) {
        try {
            auto result = handler.transform(key, value);
            if (!result) {
                VLOG(2) << "c2v message dropped by transform";
                stats_.c2v_messages_dropped++;
                return;
            }
            vehicle_id = result->vehicle_id;
            payload = result->payload;
        } catch (const std::exception& e) {
            LOG(ERROR) << "c2v transform error for topic=" << kafka_topic
                       << ": " << e.what();
            stats_.c2v_transform_errors++;
            return;
        }
    } else {
        // Default: key is vehicle_id, value is payload
        vehicle_id = key;
        payload = value;
    }

    if (vehicle_id.empty()) {
        LOG(WARNING) << "c2v message has empty vehicle_id, dropping";
        stats_.c2v_messages_dropped++;
        return;
    }

    stats_.c2v_messages_transformed++;

    // Build MQTT topic and publish
    std::string mqtt_topic = build_c2v_topic(vehicle_id, handler.content_id);

    LOG(INFO) << "c2v: " << kafka_topic << " -> " << mqtt_topic
              << " (" << payload.size() << " bytes)";

    impl_->publish_mqtt(mqtt_topic, payload);
}

}  // namespace ifex::offboard
