#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace ifex::offboard {

/// Vehicle metadata for enrichment (loaded from reference topic)
struct VehicleContext {
    std::string vehicle_id;
    std::string fleet_id;
    std::string region;
    std::string customer_id;

    // Additional metadata fields can be added here
    std::unordered_map<std::string, std::string> attributes;

    // When this context was last updated
    std::chrono::steady_clock::time_point last_updated;
};

/// Thread-safe store for vehicle contexts
class VehicleContextStore {
public:
    VehicleContextStore() = default;

    /// Update or insert vehicle context
    void upsert(const std::string& vehicle_id, VehicleContext ctx);

    /// Get vehicle context (returns nullopt if not found)
    std::optional<VehicleContext> get(const std::string& vehicle_id) const;

    /// Check if vehicle exists
    bool exists(const std::string& vehicle_id) const;

    /// Remove vehicle context
    void remove(const std::string& vehicle_id);

    /// Get count of stored contexts
    size_t size() const;

    /// Clear all contexts
    void clear();

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, VehicleContext> contexts_;
};

}  // namespace ifex::offboard
