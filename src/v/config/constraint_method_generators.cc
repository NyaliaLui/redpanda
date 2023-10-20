/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/constraint_method_generators.h"

#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "vassert.h"

#include <seastar/util/log.hh>

#include <algorithm>

namespace config {
inline ss::logger logger{"constraints"};

namespace {

template<typename RangeMinT, typename T>
bool less_than_min(const std::optional<RangeMinT> min, const T& topic_val) {
    return min && topic_val < static_cast<T>(*min);
}

template<typename RangeMaxT, typename T>
bool greater_than_max(const std::optional<RangeMaxT> max, const T& topic_val) {
    return max && topic_val > static_cast<T>(*max);
}

template<typename RangeT, typename T>
bool is_invalid_range(const range_values<RangeT>& range, const T& topic_val) {
    return less_than_min<RangeT, T>(range.min, topic_val)
           || greater_than_max<RangeT, T>(range.max, topic_val);
}

template<typename RangeT>
range_values<RangeT>
get_range(const constraint_args& args, const model::topic& topic) {
    // NOTE to reviewers: Not using visit here or else need to check for
    // signed/unsigned int comparison which is avoidable.
    try {
        return std::get<range_values<RangeT>>(args.flags);
    } catch (const std::bad_variant_access& ex) {
        // This should not happen since the variant is hardcoded in
        // constraint_types.cc, but just-in-case, let's catch and log.
        logger.error(
          "Constraints failure[min/max not found]: topic property {}.{}",
          topic(),
          args.name);
        return range_values<RangeT>{};
    }
}

template<typename RangeT, typename T>
bool validate_range(
  const T& topic_val, const model::topic& topic, const constraint_args& args) {
    range_values<RangeT> range = get_range<RangeT>(args, topic);
    if (is_invalid_range<RangeT, T>(range, topic_val)) {
        logger.error(
          "Constraints failure[value out-of-range]: topic property "
          "{}.{}, value {}",
          topic(),
          args.name,
          topic_val);
        return false;
    }

    return true;
}

template<typename RangeT, typename T>
std::optional<T> get_min_max(const range_values<RangeT>& range, T& topic_val) {
    if (less_than_min<RangeT, T>(range.min, topic_val)) {
        return std::make_optional(static_cast<T>(*range.min));
    }

    if (greater_than_max<RangeT, T>(range.max, topic_val)) {
        return std::make_optional(static_cast<T>(*range.max));
    }

    return std::nullopt;
}

template<typename RangeT, typename T>
void maybe_clamp_range(
  T& topic_val, const model::topic& topic, const constraint_args& args) {
    range_values<RangeT> range = get_range<RangeT>(args, topic);
    auto min_max = get_min_max<RangeT, T>(range, topic_val);
    if (min_max) {
        logger.warn(
          "Overwriting topic property {}.{} to the constraint min/max",
          topic(),
          args.name);
        topic_val = *min_max;
    }
}

template<typename T>
bool match_cluster_property(
  const T& topic_val,
  const T& cluster_val,
  const std::variant<
    range_values<int64_t>,
    range_values<uint64_t>,
    constraint_enabled_t>& flags) {
    return ss::visit(
      flags,
      [](const range_values<int64_t>) { return false; },
      [](const range_values<uint64_t>) { return false; },
      [&topic_val, &cluster_val](const constraint_enabled_t enabled) {
          return enabled == constraint_enabled_t::yes
                 && topic_val == cluster_val;
      });
}

template<typename T>
bool validate_property(
  const T& topic_val,
  const T& cluster_val,
  const model::topic& topic,
  const constraint_args& args) {
    if (!match_cluster_property(topic_val, cluster_val, args.flags)) {
        logger.error(
          "Constraints failure[does not match the cluster default {}]: "
          "topic property {}.{}, value {}",
          cluster_val,
          topic(),
          args.name,
          topic_val);
        return false;
    }

    return true;
}

template<typename T>
void maybe_clamp_property(
  T& topic_val,
  const T& cluster_val,
  const model::topic& topic,
  const constraint_args& args) {
    if (!match_cluster_property(topic_val, cluster_val, args.flags)) {
        logger.warn(
          "Overwriting topic property {}.{} to the cluster default {}",
          topic(),
          args.name,
          cluster_val);
        topic_val = cluster_val;
    }
}

template<typename Func1, typename Func2>
constraint_validator_t
make_validator(Func1&& get_topic_val, Func2&& get_cluster_val) {
    return [get_topic_val{std::forward<Func1>(get_topic_val)},
            get_cluster_val{std::forward<Func2>(get_cluster_val)}](
             const cluster::topic_configuration& topic_cfg,
             const constraint_args& args) {
        auto topic_val = get_topic_val(topic_cfg);
        return conditional_property_check(
          config::shard_local_cfg().get(args.name),
          [&topic_val, &topic_cfg, &args] {
              return validate_range<int64_t>(
                topic_val, topic_cfg.tp_ns.tp, args);
          },
          [&topic_val, &topic_cfg, &args] {
              return validate_range<uint64_t>(
                topic_val, topic_cfg.tp_ns.tp, args);
          },
          [&topic_val, &topic_cfg, &args, &get_cluster_val] {
              // For property with enabled constraint
              return validate_property(
                topic_val, get_cluster_val(), topic_cfg.tp_ns.tp, args);
          });
    };
}

template<typename Func1>
constraint_validator_t make_validator(Func1&& get_topic_val) {
    return make_validator(
      get_topic_val, [get_cluster_val{std::forward<Func1>(get_topic_val)}] {
          return get_cluster_val(cluster::topic_configuration{});
      });
}

template<typename Func1, typename Func2, typename Func3>
constraint_clamper_t make_clamper(
  Func1&& get_topic_val, Func2&& get_cluster_val, Func3&& topic_clamper) {
    return [get_topic_val{std::forward<Func1>(get_topic_val)},
            get_cluster_val{std::forward<Func2>(get_cluster_val)},
            topic_clamper{std::forward<Func3>(topic_clamper)}](
             cluster::topic_configuration& topic_cfg,
             const constraint_args& args) {
        auto topic_val = get_topic_val(topic_cfg);
        conditional_property_check(
          config::shard_local_cfg().get(args.name),
          [&topic_val, &topic_cfg, &args] {
              maybe_clamp_range<int64_t>(topic_val, topic_cfg.tp_ns.tp, args);
          },
          [&topic_val, &topic_cfg, &args] {
              maybe_clamp_range<uint64_t>(topic_val, topic_cfg.tp_ns.tp, args);
          },
          [&topic_val, &topic_cfg, &args, &get_cluster_val] {
              // For properties with enabled constraint
              maybe_clamp_property(
                topic_val, get_cluster_val(), topic_cfg.tp_ns.tp, args);
          });
        topic_clamper(topic_cfg, topic_val);
    };
}

template<typename Func1, typename Func3>
constraint_clamper_t
make_clamper(Func1&& get_topic_val, Func3&& topic_clamper) {
    return make_clamper(
      get_topic_val,
      [get_cluster_val{std::forward<Func1>(get_topic_val)}] {
          return get_cluster_val(cluster::topic_configuration{});
      },
      topic_clamper);
}
} // namespace

constraint_methods segment_size_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.segment_size.value_or(
          config::shard_local_cfg().log_segment_size.default_value());
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.properties.segment_size = topic_val;
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods retention_ms_constraint_methods() {
    vassert(
      config::shard_local_cfg().log_retention_ms.default_value().has_value(),
      "{} does not have a default value",
      config::shard_local_cfg().log_retention_ms.name());

    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.retention_duration.has_optional_value()
                 ? topic_cfg.properties.retention_duration.value()
                 : *config::shard_local_cfg().log_retention_ms.default_value();
    };
    auto clamp_topic_val = [](
                             cluster::topic_configuration& topic_cfg,
                             const std::chrono::milliseconds& topic_val) {
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(
            std::make_optional(std::move(topic_val)));
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods segment_ms_constraint_methods() {
    vassert(
      config::shard_local_cfg().log_segment_ms.default_value().has_value(),
      "{} does not have a default value",
      config::shard_local_cfg().log_segment_ms.name());

    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.segment_ms.has_optional_value()
                 ? topic_cfg.properties.segment_ms.value()
                 : *config::shard_local_cfg().log_segment_ms.default_value();
    };
    auto clamp_topic_val = [](
                             cluster::topic_configuration& topic_cfg,
                             const std::chrono::milliseconds& topic_val) {
        topic_cfg.properties.segment_ms = tristate<std::chrono::milliseconds>(
          std::make_optional(std::move(topic_val)));
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods retention_bytes_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        // TODO(@NyaliaLui): Make core thread proposing a default value for
        // cluster-level retention bytes. Hardcoding 0 value is hazardous.
        return topic_cfg.properties.retention_bytes.has_optional_value()
                 ? topic_cfg.properties.retention_bytes.value()
                 : 0;
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.properties.retention_bytes = tristate(
          std::make_optional<T>(std::move(topic_val)));
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods kafka_batch_max_bytes_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.batch_max_bytes.value_or(
          config::shard_local_cfg().kafka_batch_max_bytes.default_value());
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.properties.batch_max_bytes = topic_val;
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods retention_local_target_bytes_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        // TODO(@NyaliaLui): Make core thread proposing a default value for
        // cluster-level retention local target bytes. Hardcoding 0 value is
        // hazardous.
        return topic_cfg.properties.retention_local_target_bytes
                   .has_optional_value()
                 ? topic_cfg.properties.retention_local_target_bytes.value()
                 : 0;
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.properties.retention_local_target_bytes = tristate(
          std::make_optional<T>(std::move(topic_val)));
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods retention_local_target_ms_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.retention_local_target_ms
                   .has_optional_value()
                 ? topic_cfg.properties.retention_local_target_ms.value()
                 : config::shard_local_cfg()
                     .retention_local_target_ms_default.default_value();
    };
    auto clamp_topic_val = [](
                             cluster::topic_configuration& topic_cfg,
                             const std::chrono::milliseconds& topic_val) {
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(
            std::make_optional(std::move(topic_val)));
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods replication_factor_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.replication_factor;
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.replication_factor = topic_val;
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods partition_count_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.partition_count;
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.partition_count = topic_val;
    };
    return constraint_methods{
      make_validator(get_topic_val),
      make_clamper(get_topic_val, clamp_topic_val)};
}

constraint_methods cleanup_policy_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.cleanup_policy_bitflags.value_or(
          model::cleanup_policy_bitflags::none);
    };
    auto get_cluster_val = []() {
        return config::shard_local_cfg().log_cleanup_policy();
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.properties.cleanup_policy_bitflags = topic_val;
    };
    return constraint_methods{
      make_validator(get_topic_val, get_cluster_val),
      make_clamper(get_topic_val, get_cluster_val, clamp_topic_val)};
}

constraint_methods compression_type_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.compression.value_or(
          model::compression::none);
    };
    auto get_cluster_val = []() {
        return config::shard_local_cfg().log_compression_type();
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.properties.compression = topic_val;
    };
    return constraint_methods{
      make_validator(get_topic_val, get_cluster_val),
      make_clamper(get_topic_val, get_cluster_val, clamp_topic_val)};
}

constraint_methods timestamp_type_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.timestamp_type.value_or(
          config::shard_local_cfg().log_message_timestamp_type.default_value());
    };
    auto get_cluster_val = []() {
        return config::shard_local_cfg().log_message_timestamp_type();
    };
    auto clamp_topic_val = []<typename T>(
                             cluster::topic_configuration& topic_cfg,
                             const T& topic_val) {
        topic_cfg.properties.timestamp_type = topic_val;
    };
    return constraint_methods{
      make_validator(get_topic_val, get_cluster_val),
      make_clamper(get_topic_val, get_cluster_val, clamp_topic_val)};
}

namespace {
// Temporary replacement for model::shadow_indexing_mode::drop_fetch
model::shadow_indexing_mode drop_fetch(model::shadow_indexing_mode mode) {
    if (mode == model::shadow_indexing_mode::full) {
        return model::shadow_indexing_mode::archival;
    } else if (mode == model::shadow_indexing_mode::fetch) {
        return model::shadow_indexing_mode::disabled;
    } else {
        return mode;
    }
}

// Temporary replacement for model::shadow_indexing_mode::drop_archival
model::shadow_indexing_mode drop_archival(model::shadow_indexing_mode mode) {
    if (mode == model::shadow_indexing_mode::full) {
        return model::shadow_indexing_mode::fetch;
    } else if (mode == model::shadow_indexing_mode::archival) {
        return model::shadow_indexing_mode::disabled;
    } else {
        return mode;
    }
}

void set_si_based_on_fetch(
  std::optional<model::shadow_indexing_mode>& si,
  bool fetch_enabled,
  bool enable_remote_write) {
    if (fetch_enabled) {
        if (si) {
            si = model::add_shadow_indexing_flag(
              *si, model::shadow_indexing_mode::fetch);
        } else {
            si = enable_remote_write ? model::shadow_indexing_mode::full
                                     : model::shadow_indexing_mode::fetch;
        }
    } else {
        if (si) {
            si = drop_fetch(*si);
        } else {
            si = enable_remote_write ? model::shadow_indexing_mode::archival
                                     : model::shadow_indexing_mode::disabled;
        }
    }
}

void set_si_based_on_archival(
  std::optional<model::shadow_indexing_mode>& si,
  bool archival_enabled,
  bool enable_remote_read) {
    if (archival_enabled) {
        if (si) {
            si = model::add_shadow_indexing_flag(
              *si, model::shadow_indexing_mode::archival);
        } else {
            si = enable_remote_read ? model::shadow_indexing_mode::full
                                    : model::shadow_indexing_mode::archival;
        }
    } else {
        if (si) {
            si = drop_archival(*si);
        } else {
            si = enable_remote_read ? model::shadow_indexing_mode::fetch
                                    : model::shadow_indexing_mode::disabled;
        }
    }
}
} // namespace

constraint_methods remote_read_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.shadow_indexing ? model::is_fetch_enabled(
                 *topic_cfg.properties.shadow_indexing)
                                                    : false;
    };
    auto get_cluster_val = []() {
        return config::shard_local_cfg().cloud_storage_enable_remote_read();
    };
    auto clamp_topic_val =
      [](cluster::topic_configuration& topic_cfg, const bool& topic_val) {
          set_si_based_on_fetch(
            topic_cfg.properties.shadow_indexing,
            topic_val,
            config::shard_local_cfg().cloud_storage_enable_remote_write());
      };
    return constraint_methods{
      make_validator(get_topic_val, get_cluster_val),
      make_clamper(get_topic_val, get_cluster_val, clamp_topic_val)};
}

constraint_methods remote_write_constraint_methods() {
    auto get_topic_val = [](const cluster::topic_configuration& topic_cfg) {
        return topic_cfg.properties.shadow_indexing
                 ? model::is_archival_enabled(
                   *topic_cfg.properties.shadow_indexing)
                 : false;
    };
    auto get_cluster_val = []() {
        return config::shard_local_cfg().cloud_storage_enable_remote_write();
    };
    auto clamp_topic_val =
      [](cluster::topic_configuration& topic_cfg, const bool& topic_val) {
          set_si_based_on_archival(
            topic_cfg.properties.shadow_indexing,
            topic_val,
            config::shard_local_cfg().cloud_storage_enable_remote_read());
      };
    return constraint_methods{
      make_validator(get_topic_val, get_cluster_val),
      make_clamper(get_topic_val, get_cluster_val, clamp_topic_val)};
}
} // namespace config
