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

#include "config/constraint_store.h"

namespace config {
/**
 * Creates constraints based on the constraint mapping from the
 * configuration store. This implies that the broker already read the YAML
 * config file.
 */
void constraint_store::load_constraints() {
    load_constraints(
      config::shard_local_cfg().constraints(), config::shard_local_cfg());
}

void constraint_store::load_constraints(
  const std::unordered_map<constraint_args::key_type, constraint_args>&
    constraint_args,
  config::config_store& conf) {
    _constraints.reserve(constraint_args.size());
    for (const auto& [name, args] : constraint_args) {
        auto& property = conf.get(name);
        constraint cons{std::move(args), property.make_constraint_methods()};
        _constraints.insert_or_assign(name, std::move(cons));
    }
}

constraint_store& shard_local_constraints() {
    static thread_local constraint_store constraints;
    return constraints;
}
} // namespace config
