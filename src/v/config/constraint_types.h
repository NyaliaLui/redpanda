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

#include "config/base_property.h"
#include "config/convert.h"
#include "config/from_string_view.h"
#include "json/json.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <yaml-cpp/node/node.h>

#include <optional>
#include <string>

namespace cluster {
struct topic_configuration;
} // namespace cluster

namespace config {
enum class constraint_type {
    restrikt = 0,
    clamp = 1,
};

std::string_view to_string_view(constraint_type type);

template<>
std::optional<constraint_type>
from_string_view<constraint_type>(std::string_view sv);

using constraint_enabled_t = ss::bool_class<struct constraint_enabled_tag>;

namespace {
template<typename T>
struct range_values {
    std::optional<T> min;
    std::optional<T> max;

    range_values() = default;
    range_values(std::optional<T> min_opt, std::optional<T> max_opt)
      : min{std::move(min_opt)}
      , max{std::move(max_opt)} {}

    ss::sstring to_sstring() {
        return ssx::sformat("min: {}, max: {}", min, max);
    }

    friend bool operator==(const range_values&, const range_values&) = default;
    friend bool operator!=(const range_values&, const range_values&) = default;
};

template<typename SignedFunc, typename UnsignedFunc, typename ElseFunc>
auto conditional_property_check(
  const base_property& property,
  SignedFunc&& signed_condition,
  UnsignedFunc&& unsigned_condition,
  ElseFunc&& else_condition) {
    if (property.is_signed() || property.is_milliseconds()) {
        return signed_condition();
    } else if (property.is_unsigned() && !property.is_bool()) {
        return unsigned_condition();
    } else {
        return else_condition();
    }
}
} // namespace

struct constraint_args {
    using key_type = ss::sstring;

    ss::sstring name;
    constraint_type type;
    /**
     * TODO(@NyaliaLui): Consider a variant for the args flags (min/max and
     * enabled). We want a type defined way that forces either min/max or
     * enabled but not both. Currently, the validator enforces this but that's a
     * soft enforcement.
     */
    std::variant<
      range_values<int64_t>,
      range_values<uint64_t>,
      constraint_enabled_t>
      flags;

    static ss::sstring key_name() { return "name"; }

    const ss::sstring& key() const { return name; }

    friend bool operator==(const constraint_args&, const constraint_args&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const constraint_args& args);
};

using constraint_validator_t = ss::noncopyable_function<bool(
  const cluster::topic_configuration&, const constraint_args&)>;
using constraint_clamper_t = ss::noncopyable_function<void(
  cluster::topic_configuration&, const constraint_args&)>;

inline constexpr auto constraint_validator_always_true =
  [](const cluster::topic_configuration&, const constraint_args&) {
      return true;
  };
inline constexpr auto constraint_clamper_noop =
  [](cluster::topic_configuration&, const constraint_args&) {};

struct constraint_methods {
    constraint_validator_t validator{constraint_validator_always_true};
    constraint_clamper_t clamper{constraint_clamper_noop};
    bool is_supported{false};

    constraint_methods() = default;
    constraint_methods(constraint_validator_t, constraint_clamper_t);

    constraint_methods(const constraint_methods&) = delete;
    constraint_methods& operator=(const constraint_methods&) = delete;

    constraint_methods(constraint_methods&&) noexcept = default;
    constraint_methods& operator=(constraint_methods&&) noexcept = default;
};

using make_constraint_methods_h
  = ss::noncopyable_function<constraint_methods()>;

class constraint {
public:
    constraint(constraint_args, constraint_methods);
    ~constraint() = default;

    constraint(const constraint&) = delete;
    constraint& operator=(const constraint&) = delete;

    constraint(constraint&&) noexcept = default;
    constraint& operator=(constraint&&) noexcept = default;

    const ss::sstring& name() const;
    const constraint_type& type() const;
    bool is_supported() const;

    bool validate(const cluster::topic_configuration&) const;
    void clamp(cluster::topic_configuration&) const;

private:
    constraint_args _args;
    constraint_methods _methods;
};
} // namespace config

namespace YAML {
template<>
struct convert<config::constraint_args> {
    using type = config::constraint_args;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_enabled_t& ep);
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_args& ep);

} // namespace json
