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

#pragma once

#include "config/configuration.h"
#include "config/constraint_types.h"

#include <unordered_map>

namespace config {
/**
 * A store to track configuration constraints. This assists with de-coupling
 * constraint arguments from their methods that are used to validate or clamp
 * topic properties. Additionally, adding and searching for constraints is
 * easier.
 */
class constraint_store {
public:
    ~constraint_store() = default;

    constraint& get(const ss::sstring& name);

    /**
     * Creates constraints based on the constraint mapping from the
     * configuration store. This implies that the broker already read the YAML
     * config file.
     */
    void load_constraints();
    void load_constraints(
      const std::unordered_map<constraint_args::key_type, constraint_args>&
        constraint_args,
      config::config_store& conf);

private:
    std::unordered_map<std::string_view, constraint> _constraints;
};

constraint_store& shard_local_constraints();
} // namespace config
