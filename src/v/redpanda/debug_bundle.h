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

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <filesystem>

static constexpr ss::shard_id debug_bundle_shard_id = 0;

class debug_bundle : public ss::peering_sharded_service<debug_bundle> {
public:
    explicit debug_bundle(const std::filesystem::path& write_dir);

    ss::future<> stop();

private:
    const std::filesystem::path _write_dir;
};
