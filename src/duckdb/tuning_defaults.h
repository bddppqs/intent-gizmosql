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

#pragma once

namespace gizmosql {

// Build-time feature flags of the server's request path (and of the client's statement
// round trip). Each flag enables one mechanism; with a flag set to false, the code takes
// the path it would take without that mechanism.

// A statement's handle and execution id come from one random generator per thread.
static constexpr bool kThreadLocalUuid = true;
// The statement-text fast paths: a text without the GIZMOSQL_ prefix skips the function
// rewrite, and a text that cannot start with KILL skips the kill-session parse.
static constexpr bool kStatementTextFastPaths = true;
// A statement runs on the process-wide statement executor instead of std::async.
static constexpr bool kStatementExecutorPool = true;
// GetFlightInfo prepares a statement once and DoGet executes the prepared statement.
static constexpr bool kPrepareOnce = true;
// A bounded cache of optimized plans for repeated identical read-only statements.
static constexpr bool kPlanCache = true;
// The plan cache key carries the embedded engine's dictionary publication version.
static constexpr bool kPlanCacheDictionaryVersion = true;
// When the plan cache holds a finished statement's plan, its hash aggregate states are
// destroyed on the teardown thread.
static constexpr bool kAsyncTeardown = true;
// The client executes a statement with one RPC (DoGet on the statement) instead of
// GetFlightInfo followed by DoGet.
static constexpr bool kOneRpcPerStatement = true;

}  // namespace gizmosql
