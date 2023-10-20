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

#include "config/constraint_types.h"

namespace config {
constraint_methods segment_size_constraint_methods();
constraint_methods cleanup_policy_constraint_methods();
constraint_methods retention_ms_constraint_methods();
constraint_methods segment_ms_constraint_methods();
constraint_methods retention_bytes_constraint_methods();
constraint_methods compression_type_constraint_methods();
constraint_methods kafka_batch_max_bytes_constraint_methods();
constraint_methods timestamp_type_constraint_methods();
constraint_methods remote_read_constraint_methods();
constraint_methods remote_write_constraint_methods();
constraint_methods retention_local_target_bytes_constraint_methods();
constraint_methods retention_local_target_ms_constraint_methods();
constraint_methods replication_factor_constraint_methods();
constraint_methods partition_count_constraint_methods();
} // namespace config
