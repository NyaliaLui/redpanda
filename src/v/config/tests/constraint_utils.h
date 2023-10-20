// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/constraint_types.h"

#include <chrono>
#include <optional>
#include <unordered_map>

using namespace std::chrono_literals;

namespace {

constexpr static int64_t LIMIT_MIN = 3;
constexpr static int64_t LIMIT_MAX = 10;
constexpr static std::chrono::milliseconds LIMIT_MIN_MS = 1s;
constexpr static std::chrono::milliseconds LIMIT_MAX_MS = 5s;

std::unordered_map<config::constraint_args::key_type, config::constraint_args>
make_constraint_args(config::constraint_type type) {
    auto integral_constraint_args = config::constraint_args{
      .name = "default_topic_replications",
      .type = type,
      .flags = config::range_values<int64_t>(LIMIT_MIN, LIMIT_MAX)};
    auto enum_constraint_args = config::constraint_args{
      .name = "log_cleanup_policy",
      .type = type,
      .flags = config::constraint_enabled_t::yes};
    auto ms_constraint_args = config::constraint_args{
      .name = "log_retention_ms",
      .type = type,
      .flags = config::range_values<int64_t>(
        LIMIT_MIN_MS.count(), LIMIT_MAX_MS.count())};

    std::
      unordered_map<config::constraint_args::key_type, config::constraint_args>
        res;
    res.reserve(3);
    res[integral_constraint_args.name] = std::move(integral_constraint_args);
    res[enum_constraint_args.name] = std::move(enum_constraint_args);
    res[ms_constraint_args.name] = std::move(ms_constraint_args);
    return res;
}

} // namespace
