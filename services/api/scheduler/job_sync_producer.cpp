#include "job_sync_producer.hpp"

#include <glog/logging.h>
#include <librdkafka/rdkafkacpp.h>
#include <chrono>

#include "scheduler-sync-v2.pb.h"

namespace ifex {
namespace cloud {
namespace scheduler {

namespace sync_v2 = swdv::scheduler_sync_v2;

class JobSyncProducer::Impl : public RdKafka::DeliveryReportCb {
public:
    Impl(JobSyncProducer* producer, JobSyncProducerConfig config)
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

        LOG(INFO) << "Job sync producer initialized, topic=" << config_.topic;
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
            0,
            nullptr
        );

        if (err != RdKafka::ERR_NO_ERROR) {
            LOG(ERROR) << "Failed to produce to " << config_.topic
                       << ": " << RdKafka::err2str(err);
            producer_->stats_.send_errors++;
            return false;
        }

        kafka_producer_->poll(0);
        return true;
    }

    void flush(int timeout_ms) {
        if (kafka_producer_) {
            kafka_producer_->flush(timeout_ms);
        }
    }

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
    JobSyncProducer* producer_;
    JobSyncProducerConfig config_;
    std::unique_ptr<RdKafka::Producer> kafka_producer_;
};

JobSyncProducer::JobSyncProducer(JobSyncProducerConfig config)
    : config_(std::move(config)),
      impl_(std::make_unique<Impl>(this, config_)) {}

JobSyncProducer::~JobSyncProducer() = default;

bool JobSyncProducer::init() {
    return impl_->init();
}

void JobSyncProducer::flush(int timeout_ms) {
    impl_->flush(timeout_ms);
}

bool JobSyncProducer::send_message(const std::string& vehicle_id,
                                    const std::string& payload) {
    return impl_->send(vehicle_id, payload);
}

bool JobSyncProducer::send_job_sync(
    const std::string& vehicle_id,
    const JobData& job) {

    // Build C2V_SyncMessage with the job state
    sync_v2::C2V_SyncMessage sync_msg;
    sync_msg.set_vehicle_id(vehicle_id);
    sync_msg.set_sync_timestamp_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    // Add the job record
    auto* record = sync_msg.add_jobs();
    record->set_job_id(job.job_id);
    record->set_title(job.title);
    record->set_service(job.service);
    record->set_method(job.method);
    record->set_parameters_json(job.parameters_json);
    record->set_scheduled_time_ms(job.scheduled_time_ms);
    record->set_recurrence_rule(job.recurrence_rule);
    record->set_end_time_ms(job.end_time_ms);
    record->set_deleted(job.deleted);
    record->set_authority(sync_v2::AUTHORITY_CLOUD);

    // Set status and paused flag
    record->set_status(sync_v2::JOB_STATUS_PENDING);
    record->set_paused(job.paused);

    // Set version vector
    auto* version = record->mutable_version();
    version->set_cloud_seq(job.cloud_seq);
    version->set_vehicle_seq(job.vehicle_seq);

    std::string payload = sync_msg.SerializeAsString();

    bool sent = send_message(vehicle_id, payload);
    if (sent) {
        stats_.sync_messages_sent++;
        LOG(INFO) << "Sent C2V_SyncMessage to " << vehicle_id
                  << ": job_id=" << job.job_id
                  << " deleted=" << (job.deleted ? "true" : "false")
                  << " paused=" << (job.paused ? "true" : "false");
    }

    return sent;
}

bool JobSyncProducer::send_trigger_job(
    const std::string& vehicle_id,
    const std::string& job_id,
    const std::string& requester_id) {

    // Build TriggerJobRequest - the only imperative command
    sync_v2::TriggerJobRequest trigger;
    trigger.set_job_id(job_id);
    trigger.set_requester_id(requester_id);

    std::string payload = trigger.SerializeAsString();

    bool sent = send_message(vehicle_id, payload);
    if (sent) {
        stats_.trigger_requests_sent++;
        LOG(INFO) << "Sent TriggerJobRequest to " << vehicle_id
                  << ": job_id=" << job_id;
    }

    return sent;
}

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
