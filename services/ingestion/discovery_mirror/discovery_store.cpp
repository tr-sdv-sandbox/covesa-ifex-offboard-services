#include "discovery_store.hpp"

#include <algorithm>
#include <sstream>

#include <glog/logging.h>
#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>

namespace ifex::offboard {

using json = nlohmann::json;

DiscoveryStore::DiscoveryStore(std::shared_ptr<PostgresClient> db)
    : db_(std::move(db)) {}

void DiscoveryStore::set_schema_request_callback(SchemaRequestCallback callback) {
    schema_request_callback_ = std::move(callback);
}

// =============================================================================
// Hash-based protocol implementation
// =============================================================================

std::vector<std::string> DiscoveryStore::process_hash_manifest(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::hash_list_t& manifest) {

    LOG(INFO) << "Processing hash manifest from " << vehicle_id
              << " with " << manifest.hashes_size() << " hashes";

    // Ensure vehicle exists
    upsert_vehicle(vehicle_id);

    // Collect hashes from hash_entry_t messages
    std::vector<std::string> hashes;
    for (const auto& entry : manifest.hashes()) {
        hashes.push_back(entry.schema_hash());
    }

    // Check which hashes are unknown
    auto unknown = get_unknown_hashes(hashes);

    // Link known hashes to this vehicle
    for (const auto& hash : hashes) {
        bool is_known = std::find(unknown.begin(), unknown.end(), hash) == unknown.end();
        if (is_known) {
            link_vehicle_schema(vehicle_id, hash);
        }
    }

    if (!unknown.empty()) {
        LOG(INFO) << "Vehicle " << vehicle_id << " has " << unknown.size()
                  << " unknown schema hashes, requesting...";

        // Request unknown schemas via callback (sends c2v message)
        if (schema_request_callback_) {
            schema_request_callback_(vehicle_id, unknown);
        }
    } else {
        LOG(INFO) << "All " << hashes.size() << " hashes from " << vehicle_id
                  << " are known (no schema request needed)";
    }

    return unknown;
}

void DiscoveryStore::process_schema_response(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::schema_map_t& schemas) {

    LOG(INFO) << "Processing schema response from " << vehicle_id
              << " with " << schemas.schemas_size() << " schemas";

    for (const auto& entry : schemas.schemas()) {
        store_schema(entry.schema_hash(), entry.ifex_schema(), vehicle_id);
        link_vehicle_schema(vehicle_id, entry.schema_hash());
    }
}

std::vector<std::string> DiscoveryStore::get_unknown_hashes(
    const std::vector<std::string>& hashes) {

    std::vector<std::string> unknown;

    for (const auto& hash : hashes) {
        auto result = db_->execute_scalar(
            "SELECT 1 FROM schema_registry WHERE schema_hash = $1",
            {hash});

        if (!result) {
            unknown.push_back(hash);
        }
    }

    return unknown;
}

void DiscoveryStore::store_schema(
    const std::string& schema_hash,
    const std::string& ifex_schema,
    const std::string& vehicle_id) {

    LOG(INFO) << "Storing schema with hash " << schema_hash.substr(0, 16) << "...";

    // Parse the IFEX YAML to extract metadata and pre-parsed JSONB
    std::string service_name, version, methods_json, structs_json, enums_json;
    parse_ifex_schema(ifex_schema, service_name, version,
                      methods_json, structs_json, enums_json);

    db_->execute(
        R"(
        INSERT INTO schema_registry (
            schema_hash, ifex_schema, service_name, version,
            methods, struct_definitions, enum_definitions,
            first_seen_at, first_vehicle_id, schema_count
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, NOW(), $8, 1)
        ON CONFLICT (schema_hash) DO UPDATE
        SET schema_count = schema_registry.schema_count + 1
        )",
        {
            schema_hash,
            ifex_schema,
            service_name,
            version,
            methods_json,
            structs_json,
            enums_json,
            vehicle_id
        });
}

void DiscoveryStore::link_vehicle_schema(
    const std::string& vehicle_id,
    const std::string& schema_hash) {

    db_->execute(
        R"(
        INSERT INTO vehicle_schemas (vehicle_id, schema_hash, last_seen_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (vehicle_id, schema_hash) DO UPDATE
        SET last_seen_at = NOW()
        )",
        {vehicle_id, schema_hash});
}

void DiscoveryStore::upsert_vehicle(const std::string& vehicle_id,
                                     const std::string& fleet_id,
                                     const std::string& region) {
    db_->execute(
        "SELECT upsert_vehicle($1)",
        {vehicle_id});

    if (!fleet_id.empty() || !region.empty()) {
        VLOG(1) << "Vehicle " << vehicle_id << " fleet=" << fleet_id
                << " region=" << region;
    }
}

void DiscoveryStore::parse_ifex_schema(
    const std::string& ifex_yaml,
    std::string& service_name,
    std::string& version,
    std::string& methods_json,
    std::string& structs_json,
    std::string& enums_json) {

    try {
        YAML::Node root = YAML::Load(ifex_yaml);

        // Extract service name and version
        service_name = root["name"] ? root["name"].as<std::string>() : "unknown";
        version = root["version"] ? root["version"].as<std::string>() : "";

        // Extract methods
        json methods_arr = json::array();
        if (root["namespaces"]) {
            for (const auto& ns : root["namespaces"]) {
                std::string ns_name = ns["name"] ? ns["name"].as<std::string>() : "default";

                if (ns["methods"]) {
                    for (const auto& method : ns["methods"]) {
                        json method_obj;
                        method_obj["namespace"] = ns_name;
                        method_obj["name"] = method["name"] ? method["name"].as<std::string>() : "";
                        method_obj["description"] = method["description"] ? method["description"].as<std::string>() : "";

                        // Extract input parameters
                        json inputs = json::array();
                        if (method["input"]) {
                            for (const auto& param : method["input"]) {
                                json param_obj;
                                param_obj["name"] = param["name"] ? param["name"].as<std::string>() : "";
                                param_obj["datatype"] = param["datatype"] ? param["datatype"].as<std::string>() : "string";
                                param_obj["description"] = param["description"] ? param["description"].as<std::string>() : "";
                                inputs.push_back(param_obj);
                            }
                        }
                        method_obj["input"] = inputs;

                        // Extract output parameters
                        json outputs = json::array();
                        if (method["output"]) {
                            for (const auto& param : method["output"]) {
                                json param_obj;
                                param_obj["name"] = param["name"] ? param["name"].as<std::string>() : "";
                                param_obj["datatype"] = param["datatype"] ? param["datatype"].as<std::string>() : "string";
                                outputs.push_back(param_obj);
                            }
                        }
                        method_obj["output"] = outputs;

                        methods_arr.push_back(method_obj);
                    }
                }
            }
        }
        methods_json = methods_arr.dump();

        // Extract structs
        json structs_obj = json::object();
        if (root["typedefs"]) {
            for (const auto& td : root["typedefs"]) {
                if (td["members"]) {
                    std::string td_name = td["name"] ? td["name"].as<std::string>() : "";
                    json members = json::array();
                    for (const auto& member : td["members"]) {
                        json member_obj;
                        member_obj["name"] = member["name"] ? member["name"].as<std::string>() : "";
                        member_obj["datatype"] = member["datatype"] ? member["datatype"].as<std::string>() : "";
                        members.push_back(member_obj);
                    }
                    structs_obj[td_name] = members;
                }
            }
        }
        structs_json = structs_obj.dump();

        // Extract enums
        json enums_obj = json::object();
        if (root["enumerations"]) {
            for (const auto& en : root["enumerations"]) {
                std::string en_name = en["name"] ? en["name"].as<std::string>() : "";
                json values = json::array();
                if (en["options"]) {
                    for (const auto& opt : en["options"]) {
                        values.push_back(opt["name"] ? opt["name"].as<std::string>() : "");
                    }
                }
                enums_obj[en_name] = values;
            }
        }
        enums_json = enums_obj.dump();

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse IFEX schema: " << e.what();
        service_name = "parse_error";
        version = "";
        methods_json = "[]";
        structs_json = "{}";
        enums_json = "{}";
    }
}

}  // namespace ifex::offboard
