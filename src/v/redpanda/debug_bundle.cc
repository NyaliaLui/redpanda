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
#include "utils/string_switch.h"
#include "vlog.h"

#include <seastar/http/exception.hh>
#include <seastar/util/log.hh>
#include <seastar/util/process.hh>

#include <fmt/format.h>

#include <optional>

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
            status = debug_bundle_status::not_running;
            return make_ready_future<consumption_result_type>(
              stop_consuming_type({}));
        }
    }
    return make_ready_future<consumption_result_type>(ss::continue_consuming{});
}

constexpr std::string_view to_string_view(debug_bundle_status bundle_status) {
    switch (bundle_status) {
    case debug_bundle_status::not_running:
        return "not-running";
    case debug_bundle_status::running:
        return "running";
    }
    return "unknown";
}

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<debug_bundle_status>
from_string_view<debug_bundle_status>(std::string_view sv) {
    return string_switch<std::optional<debug_bundle_status>>(sv)
      .match(
        to_string_view(debug_bundle_status::not_running),
        debug_bundle_status::not_running)
      .match(
        to_string_view(debug_bundle_status::running),
        debug_bundle_status::running)
      .default_match(std::nullopt);
}

debug_bundle::debug_bundle(
  const std::filesystem::path& write_dir, const std::filesystem::path& rpk_path)
  : _status{debug_bundle_status::not_running}
  , _write_dir{write_dir}
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
    if (_status == debug_bundle_status::running) {
        throw ss::httpd::base_exception(
          "Too many requests: bundle is already running",
          ss::http::reply::status_type::too_many_requests);
    }

    if (ss::this_shard_id() != debug_bundle_shard_id) {
        return container().invoke_on(
          debug_bundle_shard_id,
          [&auth_state, params{std::move(params)}](debug_bundle& b) {
              return b.start_creating_bundle(auth_state, std::move(params));
          });
    }

    auto filename = detail::make_bundle_filename(
      get_write_dir(), _in_progress_filename);
    std::vector<ss::sstring> rpk_argv{
      _rpk_cmd.string(), "debug", "bundle", "--output", filename};
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
              _status = debug_bundle_status::running;
              return ss::do_with(
                std::move(process),
                std::move(stdout),
                [this, guard{std::move(guard)}](auto& p, auto& stdout) {
                    return stdout.consume(rpk_consumer(_status))
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

ss::future<debug_bundle_status> debug_bundle::get_status() {
    if (ss::this_shard_id() != debug_bundle_shard_id) {
        co_return co_await container().invoke_on(
          debug_bundle_shard_id,
          [](debug_bundle& b) mutable { return b.get_status(); });
    }

    co_return _status;
}

ss::sstring detail::make_bundle_filename(
  const std::filesystem::path& write_dir, ss::sstring& filename) {
    auto bundle_name = _write_dir / filename;
    return bundle_name.string();
}
