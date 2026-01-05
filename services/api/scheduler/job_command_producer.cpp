#include "job_command_producer.hpp"

#include <glog/logging.h>
#include <librdkafka/rdkafkacpp.h>
#include <chrono>

#include "scheduler-command-envelope.pb.h"

namespace ifex {
namespace cloud {
namespace scheduler {

class JobCommandProducer::Impl : public RdKafka::DeliveryReportCb {
public:
    Impl(JobCommandProducer* producer, JobCommandProducerConfig config)
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

        LOG(INFO) << "Job command producer initialized, topic=" << config_.topic;
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

        producer_->stats_.commands_sent++;
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
    JobCommandProducer* producer_;
    JobCommandProducerConfig config_;
    std::unique_ptr<RdKafka::Producer> kafka_producer_;
};

JobCommandProducer::JobCommandProducer(JobCommandProducerConfig config)
    : config_(std::move(config)),
      impl_(std::make_unique<Impl>(this, config_)) {}

JobCommandProducer::~JobCommandProducer() = default;

bool JobCommandProducer::init() {
    return impl_->init();
}

void JobCommandProducer::flush(int timeout_ms) {
    impl_->flush(timeout_ms);
}

bool JobCommandProducer::send_command(const std::string& vehicle_id,
                                       const std::string& payload) {
    return impl_->send(vehicle_id, payload);
}

bool JobCommandProducer::send_create_job(
    const std::string& vehicle_id,
    const std::string& command_id,
    const std::string& job_id,
    const std::string& title,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& parameters_json,
    const std::string& scheduled_time,
    const std::string& recurrence_rule,
    const std::string& end_time,
    const std::string& requester_id) {

    namespace cmd = swdv::scheduler_command_envelope;

    cmd::scheduler_command_t command;
    command.set_command_id(command_id);
    command.set_timestamp_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    command.set_requester_id(requester_id);
    command.set_type(cmd::COMMAND_CREATE_JOB);

    auto* job = command.mutable_create_job();
    job->set_job_id(job_id);
    job->set_title(title);
    job->set_service(service_name);
    job->set_method(method_name);
    job->set_parameters_json(parameters_json);
    job->set_scheduled_time(scheduled_time);
    job->set_recurrence_rule(recurrence_rule);
    job->set_end_time(end_time);

    return send_command(vehicle_id, command.SerializeAsString());
}

bool JobCommandProducer::send_update_job(
    const std::string& vehicle_id,
    const std::string& command_id,
    const std::string& job_id,
    const std::string& title,
    const std::string& scheduled_time,
    const std::string& recurrence_rule,
    const std::string& parameters_json,
    const std::string& end_time,
    const std::string& requester_id) {

    namespace cmd = swdv::scheduler_command_envelope;

    cmd::scheduler_command_t command;
    command.set_command_id(command_id);
    command.set_timestamp_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    command.set_requester_id(requester_id);
    command.set_type(cmd::COMMAND_UPDATE_JOB);

    auto* update = command.mutable_update_job();
    update->set_job_id(job_id);
    update->set_title(title);
    update->set_scheduled_time(scheduled_time);
    update->set_recurrence_rule(recurrence_rule);
    update->set_parameters_json(parameters_json);
    update->set_end_time(end_time);

    return send_command(vehicle_id, command.SerializeAsString());
}

bool JobCommandProducer::send_delete_job(
    const std::string& vehicle_id,
    const std::string& command_id,
    const std::string& job_id,
    const std::string& requester_id) {

    namespace cmd = swdv::scheduler_command_envelope;

    cmd::scheduler_command_t command;
    command.set_command_id(command_id);
    command.set_timestamp_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    command.set_requester_id(requester_id);
    command.set_type(cmd::COMMAND_DELETE_JOB);
    command.set_delete_job_id(job_id);

    return send_command(vehicle_id, command.SerializeAsString());
}

bool JobCommandProducer::send_pause_job(
    const std::string& vehicle_id,
    const std::string& command_id,
    const std::string& job_id,
    const std::string& requester_id) {

    namespace cmd = swdv::scheduler_command_envelope;

    cmd::scheduler_command_t command;
    command.set_command_id(command_id);
    command.set_timestamp_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    command.set_requester_id(requester_id);
    command.set_type(cmd::COMMAND_PAUSE_JOB);
    command.set_pause_job_id(job_id);

    return send_command(vehicle_id, command.SerializeAsString());
}

bool JobCommandProducer::send_resume_job(
    const std::string& vehicle_id,
    const std::string& command_id,
    const std::string& job_id,
    const std::string& requester_id) {

    namespace cmd = swdv::scheduler_command_envelope;

    cmd::scheduler_command_t command;
    command.set_command_id(command_id);
    command.set_timestamp_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    command.set_requester_id(requester_id);
    command.set_type(cmd::COMMAND_RESUME_JOB);
    command.set_resume_job_id(job_id);

    return send_command(vehicle_id, command.SerializeAsString());
}

bool JobCommandProducer::send_trigger_job(
    const std::string& vehicle_id,
    const std::string& command_id,
    const std::string& job_id,
    const std::string& requester_id) {

    namespace cmd = swdv::scheduler_command_envelope;

    cmd::scheduler_command_t command;
    command.set_command_id(command_id);
    command.set_timestamp_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    command.set_requester_id(requester_id);
    command.set_type(cmd::COMMAND_TRIGGER_JOB);
    command.set_trigger_job_id(job_id);

    return send_command(vehicle_id, command.SerializeAsString());
}

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
