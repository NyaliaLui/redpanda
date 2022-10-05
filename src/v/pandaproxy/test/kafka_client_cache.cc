// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/kafka_client_cache.h"

#include "config/node_config.h"
#include "config/rest_authn_endpoint.h"
#include "kafka/client/configuration.h"
#include "pandaproxy/types.h"
#include "ssx/future-util.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <vector>

using namespace std::chrono_literals;

namespace pandaproxy {
class test_client_cache : public kafka_client_cache {
public:
    explicit test_client_cache(
      size_t max_size, ss::timer<ss::lowres_clock>& evict_timer)
      : kafka_client_cache(
        to_yaml(kafka::client::configuration{}, config::redact_secrets::no),
        max_size,
        1000ms,
        evict_timer) {}
};

struct cache_wrapper {
    ss::timer<ss::lowres_clock> evict_timer;
    ss::gate g;
    test_client_cache client_cache;

    cache_wrapper(size_t max_size)
      : client_cache{max_size, std::reference_wrapper(evict_timer)} {
        evict_timer.set_callback([this] {
            ssx::spawn_with_gate(
              g, [this] { return client_cache.evict_clients(); });
        });
    }
};
} // namespace pandaproxy

namespace pp = pandaproxy;

SEASTAR_THREAD_TEST_CASE(cache_make_client) {
    pp::cache_wrapper cache_wrap{10};
    pp::credential_t user{"red", "panda"};

    {
        // Creating a client with no authn methods results in a kafka
        // client without a principal
        pp::client_ptr client = cache_wrap.client_cache.make_client(
          user, config::rest_authn_method::none);
        BOOST_TEST(client->config().sasl_mechanism.value() == "");
        BOOST_TEST(client->config().scram_username.value() == "");
        BOOST_TEST(client->config().scram_password.value() == "");
    }

    {
        // Creating a client with http_basic authn type results
        // in a kafka client with a principal
        pp::client_ptr client = cache_wrap.client_cache.make_client(
          user, config::rest_authn_method::http_basic);
        BOOST_TEST(
          client->config().sasl_mechanism.value()
          == ss::sstring{"SCRAM-SHA-256"});
        BOOST_TEST(client->config().scram_username.value() == user.name);
        BOOST_TEST(client->config().scram_password.value() == user.pass);
    }
}

SEASTAR_THREAD_TEST_CASE(cache_fetch_or_insert) {
    size_t s = 1, max_s = 1;
    pp::credential_t user{"red", "panda"};
    pp::cache_wrapper cache_wrap{s};
    BOOST_TEST(cache_wrap.client_cache.size() == 0);
    BOOST_TEST(cache_wrap.client_cache.max_size() == max_s);

    // First fetch tests not-found path: cache.size > cache.max_size and cache
    // is empty
    pp::client_ptr client = cache_wrap.client_cache.fetch_or_insert(
      user, config::rest_authn_method::http_basic);
    BOOST_TEST(
      client->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client->config().scram_username.value() == user.name);
    BOOST_TEST(client->config().scram_password.value() == user.pass);

    // Second fetch tests found path: user password did not change
    client = cache_wrap.client_cache.fetch_or_insert(
      user, config::rest_authn_method::http_basic);
    BOOST_TEST(
      client->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client->config().scram_username.value() == user.name);
    BOOST_TEST(client->config().scram_password.value() == user.pass);

    pp::credential_t user2{user};
    user2.pass = "parrot";
    // Third fetch tests found path: user password did change
    // so any refs will have the updated password.
    pp::client_ptr client2 = cache_wrap.client_cache.fetch_or_insert(
      user2, config::rest_authn_method::http_basic);
    BOOST_TEST(
      client2->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client2->config().scram_username.value() == user.name);
    BOOST_TEST(client2->config().scram_password.value() == user2.pass);
    BOOST_TEST(
      client->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client->config().scram_username.value() == user.name);
    BOOST_TEST(client->config().scram_password.value() == user2.pass);

    user2.name = "party";
    // Fourth fetch tests not-found path: cache.size == cache.max_size and cache
    // is not empty The LRU replacement policy takes affect and an element is
    // evicted
    client2 = cache_wrap.client_cache.fetch_or_insert(
      user2, config::rest_authn_method::http_basic);
    BOOST_TEST(
      client2->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client2->config().scram_username.value() == user2.name);
    BOOST_TEST(client2->config().scram_password.value() == user2.pass);
    BOOST_TEST(cache_wrap.client_cache.size() == s);
    BOOST_TEST(cache_wrap.client_cache.max_size() == max_s);
}
