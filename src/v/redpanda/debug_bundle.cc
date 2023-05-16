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

#include "config/configuration.h"
#include "ssx/future-util.h"
#include "utils/gate_guard.h"
#include "vlog.h"

#include <seastar/http/exception.hh>
#include <seastar/util/log.hh>
#include <seastar/util/process.hh>

#include <fmt/format.h>

static ss::logger bundle_log{"debug_bundle"};

using consumption_result_type
  = debug_bundle::rpk_consumer::consumption_result_type;
using stop_consuming_type = debug_bundle::rpk_consumer::stop_consuming_type;
using tmp_buf = stop_consuming_type::tmp_buf;

ss::future<consumption_result_type>
debug_bundle::rpk_consumer::operator()(tmp_buf buf) {
    std::string str_buf{buf.begin(), buf.end()}, line;
    vlog(bundle_log.trace, "Consumer read {}", str_buf);
    std::stringstream ss(str_buf);
    while (!ss.eof()) {
        std::getline(ss, line);
        if (line.starts_with("Debug bundle saved to")) {
            vlog(bundle_log.trace, "Stop condition reached on line {}", line);
            return make_ready_future<consumption_result_type>(
              stop_consuming_type({}));
        }
    }
    return make_ready_future<consumption_result_type>(ss::continue_consuming{});
}

debug_bundle::debug_bundle(
  const std::filesystem::path& write_dir, const std::filesystem::path& rpk_path)
  : _write_dir{write_dir}
  , _in_progress_filename{"debug-bundle.zip"}
  , _rpk_cmd{rpk_path} {}

ss::future<> debug_bundle::start() {
    vlog(bundle_log.info, "Starting debug bundle ...");
    auto host_env = std::getenv("PATH");
    if (!host_env) {
        vlog(
          bundle_log.warn,
          "Failed to get 'PATH' environmental variable, the debug bundle may "
          "be incomplete due to missing dependencies");
    } else {
        _host_path = fmt::format("PATH={}", host_env);
    }
}

ss::future<> debug_bundle::stop() {
    vlog(bundle_log.info, "Stopping debug bundle ...");
    co_await _rpk_gate.close();
}

ss::future<> debug_bundle::start_creating_bundle(
  const request_auth_result& auth_state, const debug_bundle_params params) {
    if (ss::this_shard_id() != debug_bundle_shard_id) {
        return container().invoke_on(
          debug_bundle_shard_id,
          [&auth_state, params{std::move(params)}](debug_bundle& b) {
              return b.start_creating_bundle(auth_state, std::move(params));
          });
    }

    auto filename = _write_dir / _in_progress_filename;
    std::vector<ss::sstring> rpk_argv{
      _rpk_cmd.string(), "debug", "bundle", "--output", filename.string()};
    // Add SASL creds to RPK flags if SASL is enabled.
    // No need to check for sasl on the broker endpoint because that is for
    // Kafka brokers where as the bundle is managed by the Admin server only.
    if (config::shard_local_cfg().enable_sasl()) {
        rpk_argv.push_back("--user");
        rpk_argv.push_back(auth_state.get_username());
        rpk_argv.push_back("--password");
        rpk_argv.push_back(auth_state.get_password());
        rpk_argv.push_back("--sasl-mechanism");
        rpk_argv.push_back(auth_state.get_mechanism());
    }

    if (params.logs_since.has_value()) {
        rpk_argv.push_back("--logs-since");
        rpk_argv.push_back(params.logs_since.value());
    }

    if (params.logs_until.has_value()) {
        rpk_argv.push_back("--logs-until");
        rpk_argv.push_back(params.logs_until.value());
    }

    if (params.logs_size_limit.has_value()) {
        rpk_argv.push_back("--logs-size-limit");
        rpk_argv.push_back(params.logs_size_limit.value());
    }

    if (params.metrics_interval.has_value()) {
        rpk_argv.push_back("--metrics-interval");
        rpk_argv.push_back(params.metrics_interval.value());
    }

    gate_guard guard{_rpk_gate};
    ssx::background
      = ss::experimental::spawn_process(
          _rpk_cmd, {.argv = std::move(rpk_argv), .env = {_host_path}})
          .then([this, guard{std::move(guard)}](auto process) {
              auto stdout = process.stdout();
              return ss::do_with(
                std::move(process),
                std::move(stdout),
                [this, guard{std::move(guard)}](auto& p, auto& stdout) {
                    return stdout.consume(rpk_consumer())
                      .finally([this, &p, guard{std::move(guard)}]() mutable {
                          return p.wait()
                            .then([this](ss::experimental::process::wait_status
                                           wstatus) {
                                auto* exit_status = std::get_if<
                                  ss::experimental::process::wait_exited>(
                                  &wstatus);
                                if (exit_status != nullptr) {
                                    if (exit_status->exit_code != 0) {
                                        vlog(
                                          bundle_log.error,
                                          "Failed to run RPK, exit code {}",
                                          exit_status->exit_code);
                                    } else {
                                        vlog(
                                          bundle_log.debug,
                                          "RPK successfully created debug "
                                          "bundle "
                                          "{}",
                                          _in_progress_filename);
                                    }
                                } else {
                                    auto* exit_signal = std::get_if<
                                      ss::experimental::process::wait_signaled>(
                                      &wstatus);

                                    if (exit_signal == nullptr) {
                                        vlog(
                                          bundle_log.error,
                                          "Failed to run RPK and the exit "
                                          "signal is undefined. Debug bundle "
                                          "{}",
                                          _in_progress_filename);
                                    } else {
                                        vlog(
                                          bundle_log.error,
                                          "Failed to run RPK, process "
                                          "terminated with signal {}",
                                          exit_signal->terminating_signal);
                                    }
                                }
                            })
                            .finally([&p, guard{std::move(guard)}]() mutable {
                                // Make sure the process dies, first gracefully
                                // and then forcefully.
                                // Please note: Seastar reports an ignored
                                // exceptional future when SIGTERM or SIGKILL is
                                // called on an already dead process.
                                p.terminate();
                                p.kill();
                            });
                      });
                });
          });

    return ss::make_ready_future<>();
}
