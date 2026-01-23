#pragma once

#include <memory>

#include "cloud-scheduler-service.grpc.pb.h"
#include "scheduler_query.hpp"
#include "job_sync_producer.hpp"
#include "postgres_client.hpp"

namespace ifex::cloud::scheduler {

// Namespace aliases for IFEX-generated types
namespace proto = swdv::cloud_scheduler_service;

/**
 * Cloud Scheduler Service gRPC implementation (IFEX-based)
 *
 * Manages fleet-wide job scheduling via commands to vehicles.
 * Inherits from multiple IFEX-style service classes (one per method).
 */
class CloudSchedulerServiceImpl final
    : public proto::create_job_service::Service,
      public proto::update_job_service::Service,
      public proto::delete_job_service::Service,
      public proto::pause_job_service::Service,
      public proto::resume_job_service::Service,
      public proto::trigger_job_service::Service,
      public proto::get_job_service::Service,
      public proto::list_jobs_service::Service,
      public proto::get_job_executions_service::Service,
      public proto::create_fleet_job_service::Service,
      public proto::delete_fleet_job_service::Service,
      public proto::get_fleet_job_stats_service::Service,
      public proto::healthy_service::Service {
public:
    CloudSchedulerServiceImpl(
        std::shared_ptr<ifex::offboard::PostgresClient> db,
        std::shared_ptr<JobSyncProducer> producer);

    // Create a new job on a vehicle
    grpc::Status create_job(
        grpc::ServerContext* context,
        const proto::create_job_request* request,
        proto::create_job_response* response) override;

    // Update an existing job
    grpc::Status update_job(
        grpc::ServerContext* context,
        const proto::update_job_request* request,
        proto::update_job_response* response) override;

    // Delete a job
    grpc::Status delete_job(
        grpc::ServerContext* context,
        const proto::delete_job_request* request,
        proto::delete_job_response* response) override;

    // Pause a job
    grpc::Status pause_job(
        grpc::ServerContext* context,
        const proto::pause_job_request* request,
        proto::pause_job_response* response) override;

    // Resume a paused job
    grpc::Status resume_job(
        grpc::ServerContext* context,
        const proto::resume_job_request* request,
        proto::resume_job_response* response) override;

    // Trigger immediate execution of a job
    grpc::Status trigger_job(
        grpc::ServerContext* context,
        const proto::trigger_job_request* request,
        proto::trigger_job_response* response) override;

    // Get job details
    grpc::Status get_job(
        grpc::ServerContext* context,
        const proto::get_job_request* request,
        proto::get_job_response* response) override;

    // List jobs with filters
    grpc::Status list_jobs(
        grpc::ServerContext* context,
        const proto::list_jobs_request* request,
        proto::list_jobs_response* response) override;

    // Get job execution history
    grpc::Status get_job_executions(
        grpc::ServerContext* context,
        const proto::get_job_executions_request* request,
        proto::get_job_executions_response* response) override;

    // Create job on multiple vehicles
    grpc::Status create_fleet_job(
        grpc::ServerContext* context,
        const proto::create_fleet_job_request* request,
        proto::create_fleet_job_response* response) override;

    // Delete job from multiple vehicles
    grpc::Status delete_fleet_job(
        grpc::ServerContext* context,
        const proto::delete_fleet_job_request* request,
        proto::delete_fleet_job_response* response) override;

    // Get fleet-wide job statistics
    grpc::Status get_fleet_job_stats(
        grpc::ServerContext* context,
        const proto::get_fleet_job_stats_request* request,
        proto::get_fleet_job_stats_response* response) override;

    // Health check (IFEX standard method)
    grpc::Status healthy(
        grpc::ServerContext* context,
        const proto::healthy_request* request,
        proto::healthy_response* response) override;

private:
    std::string generate_command_id();
    std::string generate_job_id();

    // Map internal job status to IFEX cloud_job_status_t
    proto::cloud_job_status_t map_status(int status);

    // Map internal sync state to IFEX sync_state_t
    proto::sync_state_t map_sync_state(query::SyncState state);

    std::shared_ptr<ifex::offboard::PostgresClient> db_;
    std::shared_ptr<JobSyncProducer> producer_;
    SchedulerQuery query_;
    bool is_healthy_ = false;
};

}  // namespace ifex::cloud::scheduler
