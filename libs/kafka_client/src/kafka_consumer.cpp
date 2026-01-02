#include "kafka_consumer.hpp"

#include <glog/logging.h>

namespace ifex::offboard {

KafkaConsumer::KafkaConsumer(const KafkaConsumerConfig& config) {
    std::string errstr;

    auto conf = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    // Set configuration
    if (conf->set("bootstrap.servers", config.brokers, errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set bootstrap.servers: " << errstr;
    }

    if (conf->set("group.id", config.group_id, errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set group.id: " << errstr;
    }

    if (conf->set("client.id", config.client_id, errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set client.id: " << errstr;
    }

    if (conf->set("auto.offset.reset", config.auto_offset_reset, errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set auto.offset.reset: " << errstr;
    }

    if (conf->set("enable.auto.commit",
                  config.enable_auto_commit ? "true" : "false",
                  errstr) != RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set enable.auto.commit: " << errstr;
    }

    if (conf->set("auto.commit.interval.ms",
                  std::to_string(config.auto_commit_interval_ms),
                  errstr) != RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set auto.commit.interval.ms: " << errstr;
    }

    if (conf->set("session.timeout.ms",
                  std::to_string(config.session_timeout_ms),
                  errstr) != RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set session.timeout.ms: " << errstr;
    }

    // Create consumer
    consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
    if (!consumer_) {
        LOG(FATAL) << "Failed to create Kafka consumer: " << errstr;
    }

    LOG(INFO) << "Kafka consumer created, group=" << config.group_id
              << ", brokers=" << config.brokers;
}

KafkaConsumer::~KafkaConsumer() {
    stop();
    if (consumer_) {
        consumer_->close();
    }
}

bool KafkaConsumer::subscribe(const std::vector<std::string>& topics) {
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG(ERROR) << "Failed to subscribe: " << RdKafka::err2str(err);
        return false;
    }

    LOG(INFO) << "Subscribed to " << topics.size() << " topics";
    for (const auto& topic : topics) {
        LOG(INFO) << "  - " << topic;
    }
    return true;
}

void KafkaConsumer::unsubscribe() {
    consumer_->unsubscribe();
}

bool KafkaConsumer::poll(int timeout_ms, KafkaMessage& message) {
    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(timeout_ms));

    if (!msg) {
        return false;
    }

    switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            message.topic = msg->topic_name();
            message.partition = msg->partition();
            message.offset = msg->offset();
            message.timestamp = msg->timestamp().timestamp;

            if (msg->key()) {
                message.key = *msg->key();
            } else {
                message.key.clear();
            }

            if (msg->payload()) {
                message.value.assign(
                    static_cast<const char*>(msg->payload()),
                    msg->len());
            } else {
                message.value.clear();
            }
            return true;

        case RdKafka::ERR__TIMED_OUT:
            return false;

        case RdKafka::ERR__PARTITION_EOF:
            // End of partition, not an error
            return false;

        default:
            LOG(WARNING) << "Consumer error: " << msg->errstr();
            return false;
    }
}

void KafkaConsumer::consume_loop(MessageCallback callback, int timeout_ms) {
    running_ = true;

    while (running_) {
        KafkaMessage message;
        if (poll(timeout_ms, message)) {
            callback(message);
        }
    }
}

void KafkaConsumer::stop() {
    running_ = false;
}

bool KafkaConsumer::commit() {
    RdKafka::ErrorCode err = consumer_->commitSync();
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG(ERROR) << "Failed to commit offsets: " << RdKafka::err2str(err);
        return false;
    }
    return true;
}

bool KafkaConsumer::commit(const KafkaMessage& message) {
    std::vector<RdKafka::TopicPartition*> offsets;

    RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(
        message.topic, message.partition, message.offset + 1);
    offsets.push_back(tp);

    RdKafka::ErrorCode err = consumer_->commitSync(offsets);

    delete tp;

    if (err != RdKafka::ERR_NO_ERROR) {
        LOG(ERROR) << "Failed to commit offset: " << RdKafka::err2str(err);
        return false;
    }
    return true;
}

std::vector<std::string> KafkaConsumer::get_subscription() const {
    std::vector<std::string> topics;
    consumer_->subscription(topics);
    return topics;
}

}  // namespace ifex::offboard
