#include "enrichment_consumer.hpp"

#include <glog/logging.h>
#include <librdkafka/rdkafkacpp.h>

#include "kafka_consumer.hpp"
#include "vehicle-enrichment.pb.h"

namespace ifex::offboard {

class EnrichmentConsumer::Impl {
public:
    std::unique_ptr<KafkaConsumer> consumer;
};

EnrichmentConsumer::EnrichmentConsumer(EnrichmentConsumerConfig config,
                                       std::shared_ptr<VehicleContextStore> store)
    : config_(std::move(config)), store_(std::move(store)), impl_(std::make_unique<Impl>()) {

    // For KTable-style consumption, we don't need consumer groups
    // We manually assign all partitions from beginning
    KafkaConsumerConfig kafka_config;
    kafka_config.brokers = config_.brokers;
    kafka_config.group_id = config_.group_id;  // Still needed but not used for offset tracking
    kafka_config.client_id = config_.client_id;
    kafka_config.enable_auto_commit = false;

    impl_->consumer = std::make_unique<KafkaConsumer>(kafka_config);
}

EnrichmentConsumer::~EnrichmentConsumer() {
    stop();
}

size_t EnrichmentConsumer::load_initial() {
    LOG(INFO) << "Loading initial enrichment data from " << config_.topic;

    // Get partition count
    int32_t partition_count = impl_->consumer->get_partition_count(config_.topic);
    if (partition_count <= 0) {
        LOG(ERROR) << "Failed to get partitions for " << config_.topic;
        return 0;
    }

    // Build partition list [0, 1, 2, ...]
    std::vector<int32_t> partitions;
    for (int32_t p = 0; p < partition_count; ++p) {
        partitions.push_back(p);
    }

    // Assign all partitions from beginning
    if (!impl_->consumer->assign(config_.topic, partitions,
                                  RdKafka::Topic::OFFSET_BEGINNING)) {
        LOG(ERROR) << "Failed to assign partitions";
        return 0;
    }

    LOG(INFO) << "Assigned " << partition_count << " partitions, consuming from beginning";

    size_t count = 0;
    int empty_polls = 0;
    constexpr int MAX_EMPTY_POLLS = 3;
    auto start = std::chrono::steady_clock::now();
    auto timeout = config_.initial_load_timeout;

    // Poll until EOF on all partitions or timeout
    while (true) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed >= timeout) {
            LOG(WARNING) << "Initial load timed out after "
                         << std::chrono::duration_cast<std::chrono::seconds>(elapsed).count()
                         << "s with " << count << " records";
            break;
        }

        KafkaMessage msg;
        if (impl_->consumer->poll(500, msg)) {
            stats_.messages_received++;
            process_message(msg.key, msg.value);
            count++;
            empty_polls = 0;

            if (count % 1000 == 0) {
                LOG(INFO) << "Loaded " << count << " enrichment records...";
            }
        } else {
            empty_polls++;
            if (empty_polls >= MAX_EMPTY_POLLS) {
                break;  // Reached end of all partitions
            }
        }
    }

    initial_load_complete_ = true;
    auto elapsed = std::chrono::steady_clock::now() - start;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    double elapsed_sec = elapsed_ms / 1000.0;
    size_t throughput = (elapsed_ms > 0) ? (count * 1000 / elapsed_ms) : 0;

    LOG(INFO) << "Initial enrichment load complete: " << count << " records, "
              << store_->size() << " unique vehicles, "
              << elapsed_sec << "s, " << throughput << " records/sec";

    return count;
}

void EnrichmentConsumer::start_background() {
    if (running_) {
        LOG(WARNING) << "Enrichment consumer already running";
        return;
    }

    if (!initial_load_complete_) {
        LOG(WARNING) << "Starting background consumer before initial load - may miss updates";
    }

    running_ = true;
    background_thread_ = std::make_unique<std::thread>(&EnrichmentConsumer::background_loop, this);

    LOG(INFO) << "Enrichment consumer background thread started";
}

void EnrichmentConsumer::stop() {
    if (!running_) return;

    running_ = false;
    impl_->consumer->stop();

    if (background_thread_ && background_thread_->joinable()) {
        background_thread_->join();
    }
    background_thread_.reset();

    LOG(INFO) << "Enrichment consumer stopped";
}

size_t EnrichmentConsumer::context_count() const {
    return store_->size();
}

void EnrichmentConsumer::process_message(const std::string& key, const std::string& value) {
    if (key.empty()) {
        stats_.parse_errors++;
        return;
    }

    // Empty value = tombstone (delete)
    if (value.empty()) {
        store_->remove(key);
        stats_.tombstones_applied++;
        VLOG(1) << "Enrichment tombstone for " << key;
        return;
    }

    // Parse protobuf
    VehicleEnrichment msg;
    if (!msg.ParseFromString(value)) {
        stats_.parse_errors++;
        LOG(WARNING) << "Failed to parse enrichment protobuf for " << key;
        return;
    }

    // Build context
    VehicleContext ctx;
    ctx.vehicle_id = msg.vehicle_id();
    ctx.fleet_id = msg.fleet_id();
    ctx.region = msg.region();
    ctx.customer_id = msg.customer_id();
    ctx.last_updated = std::chrono::steady_clock::now();

    // Store additional fields as attributes
    if (!msg.model().empty()) {
        ctx.attributes["model"] = msg.model();
    }
    if (msg.year() != 0) {
        ctx.attributes["year"] = std::to_string(msg.year());
    }
    if (!msg.variant().empty()) {
        ctx.attributes["variant"] = msg.variant();
    }
    for (const auto& tag : msg.tags()) {
        ctx.attributes["tag:" + tag] = "true";
    }

    store_->upsert(key, std::move(ctx));
    stats_.updates_applied++;

    VLOG(2) << "Enrichment update for " << key << ": fleet=" << msg.fleet_id()
            << " region=" << msg.region() << " model=" << msg.model();
}

void EnrichmentConsumer::background_loop() {
    LOG(INFO) << "Enrichment consumer background loop starting";

    while (running_) {
        KafkaMessage msg;
        if (impl_->consumer->poll(config_.poll_timeout_ms, msg)) {
            stats_.messages_received++;
            process_message(msg.key, msg.value);
        }
    }

    LOG(INFO) << "Enrichment consumer background loop exiting";
}

}  // namespace ifex::offboard
