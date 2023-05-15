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
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <filesystem>

static constexpr ss::shard_id debug_bundle_shard_id = 0;

class debug_bundle : public ss::peering_sharded_service<debug_bundle> {
public:
    struct rpk_consumer {
        using consumption_result_type =
          typename ss::input_stream<char>::consumption_result_type;
        using stop_consuming_type =
          typename consumption_result_type::stop_consuming_type;
        using tmp_buf = stop_consuming_type::tmp_buf;

        ss::future<consumption_result_type> operator()(tmp_buf buf);
    };

    debug_bundle(
      const std::filesystem::path& write_dir,
      const std::filesystem::path& rpk_path);

    ss::future<> start();
    ss::future<> start_creating_bundle();
    ss::future<> stop();

private:
    const std::filesystem::path _write_dir;
    ss::sstring _in_progress_filename;
    ss::sstring _host_path;
    const std::filesystem::path _rpk_cmd;
    ss::gate _rpk_gate;
};
