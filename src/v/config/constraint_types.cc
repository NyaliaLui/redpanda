// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/constraint_types.h"

#include "cluster/types.h"
#include "config/configuration.h"

namespace config {
std::string_view to_string_view(constraint_type type) {
    switch (type) {
    case constraint_type::restrikt:
        return "restrict";
    case constraint_type::clamp:
        return "clamp";
    }
}

template<>
std::optional<constraint_type>
from_string_view<constraint_type>(std::string_view sv) {
    return string_switch<std::optional<constraint_type>>(sv)
      .match("restrict", constraint_type::restrikt)
      .match("clamp", constraint_type::clamp);
}

std::ostream& operator<<(std::ostream& os, const constraint_args& args) {
    auto msg = ss::visit(
      args.flags,
      [](config::range_values<int64_t> signed_range) {
          return signed_range.to_sstring();
      },
      [](config::range_values<uint64_t> unsigned_range) {
          return unsigned_range.to_sstring();
      },
      [](config::constraint_enabled_t enabled) {
          return ssx::sformat("enabled: {}", enabled);
      });

    fmt::print(
      os,
      "{{name: {}, type: {}, {}}}",
      args.name,
      config::to_string_view(args.type),
      msg);
    return os;
}

constraint_methods::constraint_methods(
  constraint_validator_t validator, constraint_clamper_t clamper)
  : validator{std::move(validator)}
  , clamper{std::move(clamper)}
  , is_supported{true} {}

constraint::constraint(constraint_args args, constraint_methods methods)
  : _args{std::move(args)}
  , _methods{std::move(methods)} {}

const ss::sstring& constraint::name() const { return _args.name; }
const constraint_type& constraint::type() const { return _args.type; }
bool constraint::is_supported() const { return _methods.is_supported; }

bool constraint::validate(const cluster::topic_configuration& topic_cfg) const {
    return _methods.validator(topic_cfg, _args);
}

void constraint::clamp(cluster::topic_configuration& topic_cfg) const {
    return _methods.clamper(topic_cfg, _args);
}
} // namespace config

namespace YAML {

template<typename T>
void encode_range(Node& node, config::range_values<T>& range) {
    if (range.min) {
        node["min"] = *range.min;
    }
    if (range.max) {
        node["max"] = *range.max;
    }
}

Node convert<config::constraint_args>::encode(const type& rhs) {
    Node node;
    node["name"] = rhs.name;
    node["type"] = ss::sstring(config::to_string_view(rhs.type));

    ss::visit(
      rhs.flags,
      [&node](config::range_values<int64_t> signed_range) {
          encode_range(node, signed_range);
      },
      [&node](config::range_values<uint64_t> unsigned_range) {
          encode_range(node, unsigned_range);
      },
      [&node](config::constraint_enabled_t enabled) {
          node["enabled"] = enabled == config::constraint_enabled_t::yes;
      });

    return node;
}

template<typename T>
config::range_values<T> decode_range(const Node& node) {
    config::range_values<T> range;
    if (node["min"] && !node["min"].IsNull()) {
        range.min = node["min"].as<T>();
    }

    if (node["max"] && !node["max"].IsNull()) {
        range.max = node["max"].as<T>();
    }
    return range;
}

bool convert<config::constraint_args>::decode(const Node& node, type& rhs) {
    for (const auto& s : {"name", "type"}) {
        if (!node[s]) {
            return false;
        }
    }

    auto name = node["name"].as<ss::sstring>();
    auto type = config::from_string_view<config::constraint_type>(
                  node["type"].as<ss::sstring>())
                  .value();

    config::conditional_property_check(
      config::shard_local_cfg().get(name),
      [&node, &rhs] { rhs.flags = decode_range<int64_t>(node); },
      [&node, &rhs] { rhs.flags = decode_range<uint64_t>(node); },
      [&node, &rhs] {
          if (node["enabled"] && !node["enabled"].IsNull()) {
              rhs.flags = config::constraint_enabled_t(
                node["enabled"].as<bool>());
          }
      });

    rhs.name = std::move(name);
    rhs.type = std::move(type);
    return true;
}

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_enabled_t& ep) {
    w.Bool(bool(ep));
}

template<typename T>
void rjson_serialize_range(
  json::Writer<json::StringBuffer>& w, config::range_values<T>& range) {
    if (range.min) {
        w.Key("min");
        rjson_serialize(w, range.min);
    }
    if (range.max) {
        w.Key("max");
        rjson_serialize(w, range.max);
    }
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_args& args) {
    w.StartObject();
    w.Key("name");
    w.String(args.name);
    w.Key("type");
    w.String(ss::sstring(config::to_string_view(args.type)));
    ss::visit(
      args.flags,
      [&w](config::range_values<int64_t> signed_range) {
          rjson_serialize_range(w, signed_range);
      },
      [&w](config::range_values<uint64_t> unsigned_range) {
          rjson_serialize_range(w, unsigned_range);
      },
      [&w](config::constraint_enabled_t enabled) {
          w.Key("enabled");
          rjson_serialize(w, enabled);
      });
    w.EndObject();
}

} // namespace json
