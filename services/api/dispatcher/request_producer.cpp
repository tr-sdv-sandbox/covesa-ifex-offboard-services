#include "request_producer.hpp"

#include <glog/logging.h>
#include <librdkafka/rdkafkacpp.h>

namespace ifex {
namespace cloud {
namespace dispatcher {

class RequestProducer::Impl : public RdKafka::DeliveryReportCb {
public:
    Impl(RequestProducer* producer, RequestProducerConfig config)
        : producer_(producer), config_(std::move(config)) {}

    ~Impl() {
        if (kafka_producer_) {
            kafka_producer_->flush(5000);
        }
    }

    bool init() {
        std::string errstr;
        auto conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

        conf->set("bootstrap.servers", config_.brokers, errstr);
        conf->set("client.id", config_.client_id, errstr);
        conf->set("queue.buffering.max.ms", "100", errstr);
        conf->set("batch.num.messages", "1000", errstr);
        conf->set("dr_cb", this, errstr);

        kafka_producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
        if (!kafka_producer_) {
            LOG(ERROR) << "Failed to create Kafka producer: " << errstr;
            return false;
        }

        LOG(INFO) << "Request producer initialized, topic=" << config_.topic;
        return true;
    }

    bool send(const std::string& vehicle_id, const std::string& payload) {
        if (!kafka_producer_) {
            LOG(ERROR) << "Producer not initialized";
            return false;
        }

        RdKafka::ErrorCode err = kafka_producer_->produce(
            config_.topic,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(payload.data()), payload.size(),
            vehicle_id.data(), vehicle_id.size(),
            0,       // timestamp
            nullptr  // opaque
        );

        if (err != RdKafka::ERR_NO_ERROR) {
            LOG(ERROR) << "Failed to produce to " << config_.topic
                       << ": " << RdKafka::err2str(err);
            producer_->stats_.send_errors++;
            return false;
        }

        producer_->stats_.messages_sent++;

        // Poll for delivery reports (non-blocking)
        kafka_producer_->poll(0);

        return true;
    }

    void flush(int timeout_ms) {
        if (kafka_producer_) {
            kafka_producer_->flush(timeout_ms);
        }
    }

    // Delivery report callback
    void dr_cb(RdKafka::Message& message) override {
        if (message.err()) {
            LOG(ERROR) << "Delivery failed: " << message.errstr();
            producer_->stats_.send_errors++;
        } else {
            VLOG(2) << "Delivered to " << message.topic_name()
                    << " [" << message.partition() << "]"
                    << " offset " << message.offset();
        }
    }

private:
    RequestProducer* producer_;
    RequestProducerConfig config_;
    std::unique_ptr<RdKafka::Producer> kafka_producer_;
};

RequestProducer::RequestProducer(RequestProducerConfig config)
    : config_(std::move(config)),
      impl_(std::make_unique<Impl>(this, config_)) {}

RequestProducer::~RequestProducer() = default;

bool RequestProducer::init() {
    return impl_->init();
}

bool RequestProducer::send(const std::string& vehicle_id, const std::string& payload) {
    return impl_->send(vehicle_id, payload);
}

void RequestProducer::flush(int timeout_ms) {
    impl_->flush(timeout_ms);
}

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
