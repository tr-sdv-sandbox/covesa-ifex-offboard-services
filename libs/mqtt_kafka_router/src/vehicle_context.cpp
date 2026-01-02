#include "vehicle_context.hpp"

namespace ifex::offboard {

void VehicleContextStore::upsert(const std::string& vehicle_id, VehicleContext ctx) {
    std::lock_guard<std::mutex> lock(mutex_);
    ctx.last_updated = std::chrono::steady_clock::now();
    contexts_[vehicle_id] = std::move(ctx);
}

std::optional<VehicleContext> VehicleContextStore::get(const std::string& vehicle_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = contexts_.find(vehicle_id);
    if (it != contexts_.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool VehicleContextStore::exists(const std::string& vehicle_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return contexts_.find(vehicle_id) != contexts_.end();
}

void VehicleContextStore::remove(const std::string& vehicle_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    contexts_.erase(vehicle_id);
}

size_t VehicleContextStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return contexts_.size();
}

void VehicleContextStore::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    contexts_.clear();
}

}  // namespace ifex::offboard
