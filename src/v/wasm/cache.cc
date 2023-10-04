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

#include "wasm/cache.h"

#include "wasm/logger.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/container/btree_set.h>

namespace wasm {

namespace {

/**
 * The interval at which we gc factories and engines that are no longer used.
 */
constexpr auto gc_interval = std::chrono::minutes(10);

template<typename Key, typename Value>
ss::future<> gc_btree_map(absl::btree_map<Key, ss::weak_ptr<Value>>* cache) {
    auto it = cache->begin();
    while (it != cache->end()) {
        // If the weak_ptr is `nullptr` then we can remove it from the cache.
        if (!it->second) {
            it = cache->erase(it);
        } else {
            ++it;
        }

        if (ss::need_preempt() && it != cache->end()) {
            // The iterator could have be invalidated if there was a write
            // during the yield. We'll use the ordered nature of the btree to
            // support resuming the iterator after the suspension point.
            Key checkpoint = it->first;
            co_await ss::yield();
            it = cache->lower_bound(checkpoint);
        }
    }
}

/**
 * Allows sharing an engine between multiple uses.
 *
 * Must live on a single core.
 */
class shared_engine
  : public engine
  , public ss::enable_shared_from_this<shared_engine>
  , public ss::weakly_referencable<shared_engine> {
public:
    explicit shared_engine(
      ss::shared_ptr<engine> underlying,
      ss::foreign_ptr<ss::shared_ptr<factory>> f)
      : _underlying(std::move(underlying))
      , _factory(std::move(f)) {}

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        auto u = co_await _mu.get_units();
        auto fut = co_await ss::coroutine::as_future<model::record_batch>(
          _underlying->transform(std::move(batch), probe));
        if (!fut.failed()) {
            co_return fut.get();
        }
        // Restart the engine
        try {
            co_await _underlying->stop();
            co_await _underlying->start();
        } catch (...) {
            vlog(
              wasm_log.warn,
              "failed to restart wasm engine: {}",
              std::current_exception());
        }
        std::rethrow_exception(fut.get_exception());
    }

    ss::future<> start() override {
        auto u = co_await _mu.get_units();
        if (_ref_count++ == 0) {
            co_await _underlying->start();
        }
    }
    ss::future<> stop() override {
        vassert(
          _ref_count > 0, "expected a call to start before a call to stop");
        auto u = co_await _mu.get_units();
        if (--_ref_count == 0) {
            co_await _underlying->stop();
        }
    }

    uint64_t memory_usage_size_bytes() const override {
        return _underlying->memory_usage_size_bytes();
    }

private:
    mutex _mu;
    size_t _ref_count = 0;
    ss::shared_ptr<engine> _underlying;
    // This factory reference is here to keep the cache entry alive.
    ss::foreign_ptr<ss::shared_ptr<factory>> _factory;
};

/**
 * A RAII scoped lock that ensures factory locks are deleted when there are no
 * waiters.
 */
class factory_creation_lock_guard {
public:
    factory_creation_lock_guard(const factory_creation_lock_guard&) = delete;
    factory_creation_lock_guard& operator=(const factory_creation_lock_guard&)
      = delete;
    factory_creation_lock_guard(factory_creation_lock_guard&&) noexcept
      = default;
    factory_creation_lock_guard&
    operator=(factory_creation_lock_guard&&) noexcept
      = default;

    static ss::future<factory_creation_lock_guard> acquire(
      absl::btree_map<model::offset, std::unique_ptr<mutex>>* mu_map,
      model::offset offset) {
        auto it = mu_map->find(offset);
        mutex* mu = nullptr;
        if (it == mu_map->end()) {
            auto inserted = mu_map->emplace(offset, std::make_unique<mutex>());
            vassert(inserted.second, "expected mutex to be inserted");
            mu = inserted.first->second.get();
        } else {
            mu = it->second.get();
        }
        mutex::units units = co_await mu->get_units();
        co_return factory_creation_lock_guard(
          offset, mu_map, mu, std::move(units));
    }

    ~factory_creation_lock_guard() {
        _underlying.return_all();
        // If nothing is waiting on or holding the mutex, we can remove the lock
        // from the map.
        if (_mu->ready()) {
            _mu_map->erase(_offset);
        }
    }

private:
    factory_creation_lock_guard(
      model::offset offset,
      absl::btree_map<model::offset, std::unique_ptr<mutex>>* mu_map,
      mutex* mu,
      mutex::units underlying)
      : _offset(offset)
      , _mu_map(mu_map)
      , _mu(mu)
      , _underlying(std::move(underlying)) {}

    model::offset _offset;
    absl::btree_map<model::offset, std::unique_ptr<mutex>>* _mu_map;
    mutex* _mu;
    mutex::units _underlying;
};
} // namespace

/** A cache for engines on a particular core. */
class engine_cache {
public:
    void
    put(model::offset offset, const ss::shared_ptr<shared_engine>& engine) {
        auto [_, inserted] = _cache.insert_or_assign(
          offset, engine->weak_from_this());
        vassert(inserted, "expected engine to be inserted");
    }

    ss::future<mutex::units> lock() { return _mu.get_units(); }

    ss::shared_ptr<engine> get(model::offset offset) {
        auto it = _cache.find(offset);
        if (it == _cache.end() || !it->second) {
            return nullptr;
        }
        return it->second->shared_from_this();
    }

    ss::future<> gc() { return gc_btree_map(&_cache); }

private:
    mutex _mu;
    absl::btree_map<model::offset, ss::weak_ptr<shared_engine>> _cache;
};

/**
 * A factory
 *
 * Owned by a single core (shared zero) but can be used on any core to make an
 * engine local to that core.
 */
class cached_factory
  : public factory
  , public ss::enable_shared_from_this<cached_factory>
  , public ss::weakly_referencable<cached_factory> {
public:
    cached_factory(
      ss::foreign_ptr<ss::shared_ptr<factory>> f,
      model::offset offset,
      ss::sharded<engine_cache>* e)
      : _offset(offset)
      , _underlying(std::move(f))
      , _engine_cache(e) {}

    ss::future<ss::shared_ptr<engine>> make_engine() override {
        auto engine = _engine_cache->local().get(_offset);
        // Try to grab an engine outside the lock
        if (engine) {
            co_return engine;
        }
        // Acquire the lock for this core
        auto u = co_await _engine_cache->local().lock();
        // Double check nobody created one while we were grabbing the lock.
        engine = _engine_cache->local().get(_offset);
        if (engine) {
            co_return engine;
        }
        // Create the actual engine and put it in the cache.
        //
        // The multiplexing engine keeps a foreign reference to this factory
        // because the factory exists only on a single shard and nothing is
        // expected to keep a reference to a factory after the engine is
        // created.
        auto foreign_this = co_await foreign_from_this();
        auto created = ss::make_shared<shared_engine>(
          co_await _underlying->make_engine(), std::move(foreign_this));
        _engine_cache->local().put(_offset, created);
        co_return created;
    }

    ss::future<ss::foreign_ptr<ss::shared_ptr<factory>>> foreign_from_this() {
        return ss::smp::submit_to(_underlying.get_owner_shard(), [this] {
            return ss::make_foreign<ss::shared_ptr<factory>>(
              shared_from_this());
        });
    }

private:
    model::offset _offset;
    ss::foreign_ptr<ss::shared_ptr<factory>> _underlying;
    ss::sharded<engine_cache>* _engine_cache;
};

caching_runtime::caching_runtime(std::unique_ptr<runtime> u)
  : _underlying(std::move(u))
  , _gc_timer(
      [this]() { ssx::spawn_with_gate(_gate, [this] { return do_gc(); }); }) {}

caching_runtime::~caching_runtime() = default;

ss::future<> caching_runtime::start() {
    co_await _underlying->start();
    co_await _engine_caches.start();
    _gc_timer.arm(gc_interval);
}

ss::future<> caching_runtime::stop() {
    _gc_timer.cancel();
    co_await _gate.close();
    co_await _engine_caches.stop();
    co_await _underlying->stop();
}

ss::future<ss::shared_ptr<factory>> caching_runtime::make_factory(
  model::transform_metadata meta, iobuf binary, ss::logger* logger) {
    model::offset offset = meta.source_ptr;
    // Look in the cache outside the lock
    auto it = _factory_cache.find(offset);
    if (it != _factory_cache.end() && it->second) {
        co_return it->second->shared_from_this();
    }
    auto lock = co_await factory_creation_lock_guard::acquire(
      &_factory_creation_mu_map, offset);
    // Look again in the cache with the lock
    it = _factory_cache.find(offset);
    if (it != _factory_cache.end() && it->second) {
        co_return it->second->shared_from_this();
    }
    // There is no factory and we're holding the lock,
    // time to create a new one.
    auto factory = co_await _underlying->make_factory(
      std::move(meta), std::move(binary), logger);

    // Now cache the factory and return the result.
    //
    // The underlying factory is wrapped in a foreign pointer because it could
    // be accessed and used from any core (it's expected the caller of this
    // function will wrap the factories in foreign pointers to hand out to other
    // cores, and we can't do that here because of the inheritance).
    auto cached = ss::make_shared<cached_factory>(
      ss::make_foreign(std::move(factory)), offset, &_engine_caches);
    auto [_, inserted] = _factory_cache.insert_or_assign(
      offset, cached->weak_from_this());
    vassert(inserted, "expected factory to be inserted");

    co_return cached;
}

ss::future<> caching_runtime::do_gc() {
    auto fut = co_await ss::coroutine::as_future(
      ss::when_all_succeed(gc_factories(), gc_engines()));
    if (fut.failed()) {
        vlog(
          wasm_log.warn,
          "wasm caching runtime gc failed: {}",
          fut.get_exception());
    }
    _gc_timer.arm(gc_interval);
}

ss::future<> caching_runtime::gc_factories() {
    return gc_btree_map(&_factory_cache);
}

ss::future<> caching_runtime::gc_engines() {
    return _engine_caches.invoke_on_all(&engine_cache::gc);
}

} // namespace wasm
