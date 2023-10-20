// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"
#include "config/bounded_property.h"
#include "config/configuration.h"
#include "config/constraint_method_generators.h"
#include "config/constraint_store.h"
#include "config/property.h"
#include "config/tests/constraint_utils.h"
#include "model/fundamental.h"

#include <seastar/testing/thread_test_case.hh>

namespace {

struct test_config : public config::config_store {
    config::bounded_property<int16_t> default_topic_replication;
    config::property<model::cleanup_policy_bitflags> log_cleanup_policy;
    config::retention_duration_property log_retention_ms;
    config::one_or_many_map_property<config::constraint_args>
      restrict_constraints;
    config::one_or_many_map_property<config::constraint_args> clamp_constraints;

    test_config()
      : default_topic_replication(
        *this,
        "default_topic_replications",
        "An integral property",
        {},
        1,
        {},
        std::nullopt,
        config::replication_factor_constraint_methods)
      , log_cleanup_policy(
          *this,
          "log_cleanup_policy",
          "An enum property",
          {},
          model::cleanup_policy_bitflags::deletion,
          config::property<model::cleanup_policy_bitflags>::noop_validator,
          std::nullopt,
          config::cleanup_policy_constraint_methods)
      , log_retention_ms(
          *this,
          "log_retention_ms",
          "A ms property",
          {},
          1h,
          config::property<
            std::optional<std::chrono::milliseconds>>::noop_validator,
          std::nullopt,
          config::retention_ms_constraint_methods)
      , restrict_constraints(
          *this,
          "restrict_constraints",
          "A sequence of constraints for testing configs with restrict "
          "constraint "
          "type",
          {},
          make_constraint_args(config::constraint_type::restrikt))
      , clamp_constraints(
          *this,
          "clamp_constraints",
          "A sequence of constraints for testing configs with clamp constraint "
          "type",
          {},
          make_constraint_args(config::constraint_type::clamp)) {}
};

class test_constraint_store : public config::constraint_store {
public:
    void load_constraints(
      const std::unordered_map<
        config::constraint_args::key_type,
        config::constraint_args>& constraint_args,
      config::config_store& conf) {
        config::constraint_store::load_constraints(constraint_args, conf);
    }
};

SEASTAR_THREAD_TEST_CASE(test_constraint_validation) {
    auto cfg = test_config{};
    cluster::topic_configuration topic_cfg;

    auto restrict_constraint_store = test_constraint_store{};
    restrict_constraint_store.load_constraints(cfg.restrict_constraints(), cfg);

    auto name = ss::sstring{"log_cleanup_policy"};
    auto& enum_constraint = restrict_constraint_store.get(name);
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    BOOST_CHECK(enum_constraint.validate(topic_cfg));
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    BOOST_CHECK(!enum_constraint.validate(topic_cfg));

    name = ss::sstring{"default_topic_replications"};
    auto& integral_constraint = restrict_constraint_store.get(name);
    topic_cfg.replication_factor = LIMIT_MIN + 1;
    BOOST_CHECK(integral_constraint.validate(topic_cfg));
    topic_cfg.replication_factor = LIMIT_MAX - 1;
    BOOST_CHECK(integral_constraint.validate(topic_cfg));
    topic_cfg.replication_factor = LIMIT_MIN - 1;
    BOOST_CHECK(!integral_constraint.validate(topic_cfg));
    topic_cfg.replication_factor = LIMIT_MAX + 1;
    BOOST_CHECK(!integral_constraint.validate(topic_cfg));

    name = ss::sstring{"log_retention_ms"};
    auto& ms_constraint = restrict_constraint_store.get(name);
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS + 1ms);
    BOOST_CHECK(ms_constraint.validate(topic_cfg));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS - 1ms);
    BOOST_CHECK(ms_constraint.validate(topic_cfg));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS - 1ms);
    BOOST_CHECK(!ms_constraint.validate(topic_cfg));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS + 1ms);
    BOOST_CHECK(!ms_constraint.validate(topic_cfg));
}

SEASTAR_THREAD_TEST_CASE(test_constraint_clamping) {
    auto cfg = test_config{};
    cluster::topic_configuration topic_cfg;
    auto clamp_constraint_store = test_constraint_store{};
    clamp_constraint_store.load_constraints(cfg.clamp_constraints(), cfg);

    // Check enum clamp constraint
    auto name = ss::sstring{"log_cleanup_policy"};
    auto& enum_constraint = clamp_constraint_store.get(name);
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    enum_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.log_cleanup_policy());
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    enum_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.log_cleanup_policy());

    // Check integral clamp constraint
    name = ss::sstring{"default_topic_replications"};
    auto& integral_constraint = clamp_constraint_store.get(name);
    topic_cfg.replication_factor = LIMIT_MIN + 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MIN + 1);
    topic_cfg.replication_factor = LIMIT_MAX - 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MAX - 1);
    topic_cfg.replication_factor = LIMIT_MIN - 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MIN);
    topic_cfg.replication_factor = LIMIT_MAX + 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MAX);

    // Check ms clamp constraint
    name = ss::sstring{"log_retention_ms"};
    auto& ms_constraint = clamp_constraint_store.get(name);
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS + 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MIN_MS + 1ms));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS - 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MAX_MS - 1ms));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS - 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MIN_MS));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS + 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MAX_MS));
}

} // namespace
