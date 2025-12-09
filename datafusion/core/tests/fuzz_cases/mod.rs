// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Fuzz tests are slow and gated behind the `extended_tests` feature.
// Run with: cargo test --features extended_tests
// This keeps the default test suite fast (ideally under 1 minute).

#[cfg(feature = "extended_tests")]
#[expect(clippy::needless_pass_by_value)]
mod aggregate_fuzz;
#[cfg(feature = "extended_tests")]
mod distinct_count_string_fuzz;
#[cfg(feature = "extended_tests")]
#[expect(clippy::needless_pass_by_value)]
mod join_fuzz;
#[cfg(feature = "extended_tests")]
mod merge_fuzz;
#[cfg(feature = "extended_tests")]
#[expect(clippy::needless_pass_by_value)]
mod sort_fuzz;
#[cfg(feature = "extended_tests")]
#[expect(clippy::needless_pass_by_value)]
mod sort_query_fuzz;
#[cfg(feature = "extended_tests")]
mod topk_filter_pushdown;

#[cfg(feature = "extended_tests")]
mod aggregation_fuzzer;
#[cfg(feature = "extended_tests")]
#[expect(clippy::needless_pass_by_value)]
mod equivalence;

#[cfg(feature = "extended_tests")]
mod pruning;

#[cfg(feature = "extended_tests")]
mod limit_fuzz;
#[cfg(feature = "extended_tests")]
#[expect(clippy::needless_pass_by_value)]
mod sort_preserving_repartition_fuzz;
#[cfg(feature = "extended_tests")]
mod window_fuzz;

// Utility modules - only needed when extended_tests is enabled
#[cfg(feature = "extended_tests")]
mod once_exec;
#[cfg(feature = "extended_tests")]
mod record_batch_generator;
#[cfg(feature = "extended_tests")]
mod spilling_fuzz_in_memory_constrained_env;
