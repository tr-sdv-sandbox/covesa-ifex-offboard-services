#include "kafka_producer.hpp"

#include <glog/logging.h>

namespace ifex::offboard {

class KafkaProducer::DeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    explicit DeliveryReportCb(KafkaProducer* producer) : producer_(producer) {}

    void dr_cb(RdKafka::Message& message) override {
        if (producer_->delivery_callback_) {
            std::string error;
            if (message.err()) {
                error = message.errstr();
                LOG(WARNING) << "Message delivery failed: " << error
                             << " (topic=" << message.topic_name() << ")";
            }
            producer_->delivery_callback_(
                message.topic_name(),
                message.partition(),
                message.offset(),
                error);
        }
    }

private:
    KafkaProducer* producer_;
};

KafkaProducer::KafkaProducer(const KafkaProducerConfig& config) {
    std::string errstr;

    auto conf = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    // Set configuration
    if (conf->set("bootstrap.servers", config.brokers, errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set bootstrap.servers: " << errstr;
    }

    if (conf->set("client.id", config.client_id, errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set client.id: " << errstr;
    }

    if (conf->set("queue.buffering.max.ms",
                  std::to_string(config.queue_buffering_max_ms),
                  errstr) != RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set queue.buffering.max.ms: " << errstr;
    }

    if (conf->set("message.timeout.ms",
                  std::to_string(config.message_timeout_ms),
                  errstr) != RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set message.timeout.ms: " << errstr;
    }

    if (conf->set("retries", std::to_string(config.max_retries), errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set retries: " << errstr;
    }

    // Disable Nagle's algorithm for lower latency
    if (conf->set("socket.nagle.disable", "true", errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG(WARNING) << "Failed to set socket.nagle.disable: " << errstr;
    }

    // Set delivery report callback
    dr_cb_ = std::make_unique<DeliveryReportCb>(this);
    if (conf->set("dr_cb", dr_cb_.get(), errstr) != RdKafka::Conf::CONF_OK) {
        LOG(FATAL) << "Failed to set dr_cb: " << errstr;
    }

    // Create producer
    producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
    if (!producer_) {
        LOG(FATAL) << "Failed to create Kafka producer: " << errstr;
    }

    LOG(INFO) << "Kafka producer created, brokers=" << config.brokers;
}

KafkaProducer::~KafkaProducer() {
    if (producer_) {
        flush(5000);
    }
}

bool KafkaProducer::produce(const std::string& topic,
                            const std::string& key,
                            const std::string& value) {
    return produce(topic, key, value.data(), value.size());
}

bool KafkaProducer::produce(const std::string& topic,
                            const std::string& key,
                            const void* value,
                            size_t value_len) {
retry:
    RdKafka::ErrorCode err = producer_->produce(
        topic,
        RdKafka::Topic::PARTITION_UA,  // Auto partition
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<void*>(value),
        value_len,
        key.data(),
        key.size(),
        0,      // Timestamp (0 = now)
        nullptr // Headers
    );

    if (err == RdKafka::ERR__QUEUE_FULL) {
        LOG(WARNING) << "Kafka queue full, waiting...";
        producer_->poll(1000);
        goto retry;
    }

    if (err != RdKafka::ERR_NO_ERROR) {
        LOG(ERROR) << "Failed to produce to " << topic << ": "
                   << RdKafka::err2str(err);
        return false;
    }

    return true;
}

void KafkaProducer::set_delivery_callback(DeliveryCallback callback) {
    delivery_callback_ = std::move(callback);
}

void KafkaProducer::poll(int timeout_ms) {
    producer_->poll(timeout_ms);
}

int KafkaProducer::flush(int timeout_ms) {
    RdKafka::ErrorCode err = producer_->flush(timeout_ms);
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG(WARNING) << "Flush failed: " << RdKafka::err2str(err);
    }
    return producer_->outq_len();
}

int KafkaProducer::queue_length() const {
    return producer_->outq_len();
}

}  // namespace ifex::offboard
