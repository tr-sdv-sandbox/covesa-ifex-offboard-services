#pragma once

#include <memory>

#include "cloud-scheduler-service.grpc.pb.h"
#include "scheduler_query.hpp"
#include "job_command_producer.hpp"
#include "postgres_client.hpp"

namespace ifex {
namespace cloud {
namespace scheduler {

/**
 * Cloud Scheduler Service gRPC implementation
 *
 * Manages fleet-wide job scheduling via commands to vehicles.
 */
class CloudSchedulerServiceImpl final
    : public ifex::cloud::scheduler::CloudSchedulerService::Service {
public:
    CloudSchedulerServiceImpl(
        std::shared_ptr<ifex::offboard::PostgresClient> db,
        std::shared_ptr<JobCommandProducer> producer);

    // Create a new job on a vehicle
    grpc::Status CreateJob(
        grpc::ServerContext* context,
        const CreateJobRequest* request,
        CreateJobResponse* response) override;

    // Update an existing job
    grpc::Status UpdateJob(
        grpc::ServerContext* context,
        const UpdateJobRequest* request,
        UpdateJobResponse* response) override;

    // Delete a job
    grpc::Status DeleteJob(
        grpc::ServerContext* context,
        const DeleteJobRequest* request,
        DeleteJobResponse* response) override;

    // Pause a job
    grpc::Status PauseJob(
        grpc::ServerContext* context,
        const PauseJobRequest* request,
        PauseJobResponse* response) override;

    // Resume a paused job
    grpc::Status ResumeJob(
        grpc::ServerContext* context,
        const ResumeJobRequest* request,
        ResumeJobResponse* response) override;

    // Trigger immediate execution of a job
    grpc::Status TriggerJob(
        grpc::ServerContext* context,
        const TriggerJobRequest* request,
        TriggerJobResponse* response) override;

    // Get job details
    grpc::Status GetJob(
        grpc::ServerContext* context,
        const GetJobRequest* request,
        GetJobResponse* response) override;

    // List jobs with filters
    grpc::Status ListJobs(
        grpc::ServerContext* context,
        const ListJobsRequest* request,
        ListJobsResponse* response) override;

    // Get job execution history
    grpc::Status GetJobExecutions(
        grpc::ServerContext* context,
        const GetJobExecutionsRequest* request,
        GetJobExecutionsResponse* response) override;

    // Create job on multiple vehicles
    grpc::Status CreateFleetJob(
        grpc::ServerContext* context,
        const CreateFleetJobRequest* request,
        CreateFleetJobResponse* response) override;

    // Delete job from multiple vehicles
    grpc::Status DeleteFleetJob(
        grpc::ServerContext* context,
        const DeleteFleetJobRequest* request,
        DeleteFleetJobResponse* response) override;

    // Get fleet-wide job statistics
    grpc::Status GetFleetJobStats(
        grpc::ServerContext* context,
        const GetFleetJobStatsRequest* request,
        GetFleetJobStatsResponse* response) override;

private:
    std::string generate_command_id();
    std::string generate_job_id();

    std::shared_ptr<ifex::offboard::PostgresClient> db_;
    std::shared_ptr<JobCommandProducer> producer_;
    SchedulerQuery query_;
};

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
