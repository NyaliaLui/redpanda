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

#include "hashing/murmur.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "reflection/adl.h"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

namespace cluster {

using tm_tx_hash_type = uint32_t;
constexpr tm_tx_hash_type tx_tm_hash_max
  = std::numeric_limits<tm_tx_hash_type>::max();

static tm_tx_hash_type get_default_range_size(int32_t tm_partitions_amount) {
    // integer division acts as div
    // size of last range can be larger than default range size
    // last range covers hashes up to tx_tm_hash_max
    return tx_tm_hash_max / tm_partitions_amount;
}

// Return hash of tx_id in range [0, tx_tm_hash_max]
inline tm_tx_hash_type get_tx_id_hash(const kafka::transactional_id& tx_id) {
    return murmur2(ss::sstring(tx_id).data(), ss::sstring(tx_id).size());
}

enum class tm_tx_hash_ranges_errc {
    success = 0,
    not_hosted = 1,
    intersection = 2
};

// Defines hash range as section where both ends are included
struct tm_hash_range
  : serde::
      envelope<tm_hash_range, serde::version<0>, serde::compat_version<0>> {
    tm_tx_hash_type first;
    tm_tx_hash_type last;

    tm_hash_range() = default;

    tm_hash_range(tm_tx_hash_type f, tm_tx_hash_type l)
      : first(f)
      , last(l) {}

    auto serde_fields() { return std::tie(first, last); }

    bool contains(tm_tx_hash_type v) const { return v >= first && v <= last; }

    bool contains(tm_hash_range r) const {
        return r.first >= first && r.last <= last;
    }

    bool intersects(tm_hash_range r) const {
        return (r.first >= first && r.first <= last)
               || (r.last >= first && r.last <= last) || r.contains(*this);
    }
};

inline tm_hash_range default_tm_hash_range(
  model::partition_id tm_partition, int32_t partitions_amount) {
    tm_tx_hash_type range_size = get_default_range_size(partitions_amount);
    tm_tx_hash_type hash_range_begin = range_size * tm_partition;
    tm_tx_hash_type hash_range_end = (tm_partition + 1) == partitions_amount
                                       ? tx_tm_hash_max
                                       : (range_size * (tm_partition + 1)) - 1;
    return {hash_range_begin, hash_range_end};
}

struct tm_hash_ranges_set
  : serde::envelope<
      tm_hash_ranges_set,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<tm_hash_range> ranges{};

    tm_hash_ranges_set() = default;
    tm_hash_ranges_set(std::vector<tm_hash_range>&& hr)
      : ranges(hr) {}

    auto serde_fields() { return std::tie(ranges); }

    void add_range(tm_hash_range range) {
        auto merged_to = ranges.end();
        if (range.first != 0) {
            for (auto r_it = ranges.begin(); r_it != ranges.end(); ++r_it) {
                if (r_it->last == range.first - 1) {
                    r_it->last = range.last;
                    merged_to = r_it;
                    break;
                }
            }
        }
        if (range.last != tx_tm_hash_max) {
            for (auto r_it = ranges.begin(); r_it != ranges.end(); ++r_it) {
                if (r_it->first == range.last + 1) {
                    if (merged_to != ranges.end()) {
                        merged_to->last = r_it->last;
                        ranges.erase(r_it);
                    } else {
                        r_it->first = range.first;
                        merged_to = r_it;
                    }
                    break;
                }
            }
        }
        if (merged_to == ranges.end()) {
            ranges.push_back(range);
        }
    }

    bool contains(tm_tx_hash_type v) const {
        for (const auto& r : ranges) {
            if (r.contains(v)) {
                return true;
            }
        }
        return false;
    }

    bool contains(const tm_hash_range& r) const {
        for (const auto& range : ranges) {
            if (range.contains(r)) {
                return true;
            }
        }
        return false;
    }

    bool intersects(tm_hash_range range) const {
        return std::any_of(
          ranges.begin(), ranges.end(), [range1 = range](tm_hash_range range2) {
              return range1.intersects(range2);
          });
    }
};

using repartitioning_id = named_type<int64_t, struct repartitioning_id_type>;

struct draining_txs
  : serde::envelope<draining_txs, serde::version<0>, serde::compat_version<0>> {
    repartitioning_id id;
    tm_hash_ranges_set ranges{};
    absl::btree_set<kafka::transactional_id> transactions{};

    draining_txs() = default;

    draining_txs(
      repartitioning_id id,
      tm_hash_ranges_set ranges,
      absl::btree_set<kafka::transactional_id> txs)
      : id(id)
      , ranges(std::move(ranges))
      , transactions(std::move(txs)) {}

    auto serde_fields() { return std::tie(id, ranges, transactions); }
};

struct tm_tx_hosted_transactions
  : serde::envelope<
      tm_tx_hosted_transactions,
      serde::version<0>,
      serde::compat_version<0>> {
    bool inited{false};
    tm_hash_ranges_set hash_ranges{};
    absl::btree_set<kafka::transactional_id> excluded_transactions{};
    absl::btree_set<kafka::transactional_id> included_transactions{};
    draining_txs draining{};

    tm_tx_hosted_transactions() = default;

    tm_tx_hosted_transactions(
      bool inited,
      tm_hash_ranges_set hr,
      absl::btree_set<kafka::transactional_id> et,
      absl::btree_set<kafka::transactional_id> it,
      draining_txs dr)
      : inited(inited)
      , hash_ranges(std::move(hr))
      , excluded_transactions(std::move(et))
      , included_transactions(std::move(it))
      , draining(std::move(dr)) {}

    auto serde_fields() {
        return std::tie(
          inited,
          hash_ranges,
          excluded_transactions,
          included_transactions,
          draining);
    }

    tm_tx_hash_ranges_errc
    exclude_transaction(const kafka::transactional_id& tx_id) {
        if (excluded_transactions.contains(tx_id)) {
            return tm_tx_hash_ranges_errc::success;
        }
        if (included_transactions.contains(tx_id)) {
            included_transactions.erase(tx_id);
            vassert(
              !hash_ranges.contains(get_tx_id_hash(tx_id)),
              "hash_ranges must not contain included txes");
            return tm_tx_hash_ranges_errc::success;
        }
        tm_tx_hash_type hash = get_tx_id_hash(tx_id);
        if (!hash_ranges.contains(hash)) {
            return tm_tx_hash_ranges_errc::not_hosted;
        }
        excluded_transactions.insert(tx_id);
        return tm_tx_hash_ranges_errc::success;
    }

    tm_tx_hash_ranges_errc
    include_transaction(const kafka::transactional_id& tx_id) {
        if (included_transactions.contains(tx_id)) {
            return tm_tx_hash_ranges_errc::success;
        }
        if (excluded_transactions.contains(tx_id)) {
            excluded_transactions.erase(tx_id);
            vassert(
              hash_ranges.contains(get_tx_id_hash(tx_id)),
              "hash_ranges must not contain excluded txes");
            return tm_tx_hash_ranges_errc::success;
        }
        tm_tx_hash_type hash = get_tx_id_hash(tx_id);
        if (hash_ranges.contains(hash)) {
            return tm_tx_hash_ranges_errc::intersection;
        }
        included_transactions.insert(tx_id);
        return tm_tx_hash_ranges_errc::success;
    }

    tm_tx_hash_ranges_errc add_range(tm_hash_range range) {
        if (hash_ranges.intersects(range)) {
            return tm_tx_hash_ranges_errc::intersection;
        }
        for (const auto& tx_id : excluded_transactions) {
            tm_tx_hash_type hash = get_tx_id_hash(tx_id);
            if (range.contains(hash)) {
                excluded_transactions.erase(tx_id);
            }
        }
        for (const auto& tx_id : included_transactions) {
            tm_tx_hash_type hash = get_tx_id_hash(tx_id);
            if (range.contains(hash)) {
                included_transactions.erase(tx_id);
            }
        }
        hash_ranges.add_range(range);
        return tm_tx_hash_ranges_errc::success;
    }

    bool contains(const kafka::transactional_id& tx_id) {
        if (excluded_transactions.contains(tx_id)) {
            return false;
        }
        if (included_transactions.contains(tx_id)) {
            return true;
        }
        auto tx_id_hash = get_tx_id_hash(tx_id);
        return hash_ranges.contains(tx_id_hash);
    }
};

} // namespace cluster

namespace reflection {

template<>
struct adl<cluster::tm_hash_range> {
    void to(iobuf& out, cluster::tm_hash_range&& hr) {
        reflection::serialize(out, hr.first, hr.last);
    }
    cluster::tm_hash_range from(iobuf_parser& in) {
        auto first = reflection::adl<cluster::tm_tx_hash_type>{}.from(in);
        auto last = reflection::adl<cluster::tm_tx_hash_type>{}.from(in);
        return {first, last};
    }
};

template<>
struct adl<cluster::tm_hash_ranges_set> {
    void to(iobuf& out, cluster::tm_hash_ranges_set&& hr) {
        reflection::serialize(out, hr.ranges);
    }
    cluster::tm_hash_ranges_set from(iobuf_parser& in) {
        auto ranges
          = reflection::adl<std::vector<cluster::tm_hash_range>>{}.from(in);
        return {std::move(ranges)};
    }
};

template<>
struct adl<cluster::draining_txs> {
    void to(iobuf& out, cluster::draining_txs&& dr) {
        reflection::serialize(out, dr.id, dr.ranges, dr.transactions);
    }
    cluster::draining_txs from(iobuf_parser& in) {
        auto id = reflection::adl<cluster::repartitioning_id>{}.from(in);
        auto ranges
          = reflection::adl<std::vector<cluster::tm_hash_range>>{}.from(in);
        auto txs = reflection::adl<absl::btree_set<kafka::transactional_id>>{}
                     .from(in);
        return {id, std::move(ranges), std::move(txs)};
    }
};

template<>
struct adl<cluster::tm_tx_hosted_transactions> {
    void to(iobuf& out, cluster::tm_tx_hosted_transactions&& hr) {
        reflection::serialize(
          out,
          hr.inited,
          hr.hash_ranges,
          hr.excluded_transactions,
          hr.included_transactions,
          hr.draining);
    }
    cluster::tm_tx_hosted_transactions from(iobuf_parser& in) {
        auto inited = reflection::adl<bool>{}.from(in);

        auto hash_ranges_set
          = reflection::adl<cluster::tm_hash_ranges_set>{}.from(in);
        auto included_transactions
          = reflection::adl<absl::btree_set<kafka::transactional_id>>{}.from(
            in);
        auto excluded_transactions
          = reflection::adl<absl::btree_set<kafka::transactional_id>>{}.from(
            in);
        auto draining = reflection::adl<cluster::draining_txs>{}.from(in);
        return {
          inited,
          std::move(hash_ranges_set),
          std::move(included_transactions),
          std::move(excluded_transactions),
          std::move(draining)};
    };
};

} // namespace reflection
