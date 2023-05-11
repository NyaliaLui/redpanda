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

#include "redpanda/debug_bundle.h"

#include "vlog.h"

#include <seastar/util/log.hh>

static ss::logger bundle_log{"debug_bundle"};

debug_bundle::debug_bundle(const std::filesystem::path& write_dir)
  : _write_dir{write_dir} {}

ss::future<> debug_bundle::stop() {
    vlog(bundle_log.info, "Stopping debug bundle ...");
    return ss::make_ready_future<>();
}
