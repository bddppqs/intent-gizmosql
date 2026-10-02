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

#include "duckdb_statement.h"
#include "system_catalog.h"

#include <duckdb.h>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/main/query_profiler.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>
#include <duckdb/function/table/arrow/arrow_duck_schema.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/common/enums/set_scope.hpp>
#include "duckdb/parser/expression/constant_expression.hpp"
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp>
#include <duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp>
#include <duckdb/execution/operator/aggregate/run_aggregate.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/filter/conjunction_filter.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/filter/optional_filter.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/execution/operator/aggregate/physical_partitioned_aggregate.hpp>
#include <duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp>
#include <duckdb/execution/operator/aggregate/physical_streaming_first_keys.hpp>
#include <duckdb/execution/operator/filter/physical_filter.hpp>
#include <duckdb/execution/operator/helper/physical_limit.hpp>
#include <duckdb/execution/operator/helper/physical_limit_percent.hpp>
#include <duckdb/execution/operator/helper/physical_streaming_limit.hpp>
#include <duckdb/execution/operator/join/physical_comparison_join.hpp>
#include <duckdb/execution/operator/join/physical_iejoin.hpp>
#include <duckdb/execution/operator/join/physical_nested_loop_join.hpp>
#include <duckdb/execution/operator/join/physical_piecewise_merge_join.hpp>
#include <duckdb/execution/operator/order/physical_order.hpp>
#include <duckdb/execution/operator/order/physical_top_n.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/storage/compression/dict_global/column_dictionary.hpp>
#include <openssl/evp.h>
#include <future>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <mutex>
#include <optional>
#include <regex>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <condition_variable>
#include <deque>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include <malloc.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>

#include <arrow/api.h>
#include <arrow/util/logging.h>
#include <arrow/c/bridge.h>
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/context/runtime_context.h>
#endif
#include "duckdb_server.h"
#include "session_context.h"
#include "shutdown_state.h"
#include "admin_command_guard.h"
#include "tuning_defaults.h"
#include "gizmosql_telemetry.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#ifdef GIZMOSQL_ENTERPRISE
#include "enterprise/instrumentation/instrumentation_manager.h"
#include "enterprise/instrumentation/instrumentation_records.h"
#include "enterprise/kill_session/kill_session_handler.h"
#include "enterprise/catalog_permissions/catalog_permissions_handler.h"
#include "enterprise/enterprise_features.h"
#endif
#include <nlohmann/json.hpp>
#include "version.h"
#include "gizmosql_library.h"  // GIZMOSQL_SERVER_VERSION (channel-aware)

using arrow::Status;
using duckdb::QueryResult;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
namespace context_api = opentelemetry::context;
#endif

namespace {

/// Returns true if the string is valid JSON (object, array, or primitive).
bool IsValidJSON(const std::string& s) {
  auto parsed = nlohmann::json::parse(s, nullptr, false);
  return !parsed.is_discarded();
}

bool IsLikelyGizmoSQLSet(const std::string& sql) {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  std::string upper = boost::to_upper_copy(trimmed);
  if (upper.rfind("SET ", 0) != 0 && upper.rfind("SET\t", 0) != 0 &&
      upper.rfind("SET\n", 0) != 0) {
    return false;
  }
  return upper.find("GIZMOSQL.") != std::string::npos;
}

std::string GetSqlOperationForMetrics(const std::string& sql) {
  std::string trimmed = sql;
  boost::algorithm::trim_left(trimmed);
  if (trimmed.empty()) return "UNKNOWN";

  const auto token_end = trimmed.find_first_of(" \t\r\n(;)");
  std::string operation = trimmed.substr(0, token_end);
  while (!operation.empty() &&
         !std::isalpha(static_cast<unsigned char>(operation.front()))) {
    operation.erase(operation.begin());
  }
  if (operation.empty()) return "UNKNOWN";

  boost::algorithm::to_upper(operation);
  return operation;
}

int64_t GetArrayDataSize(const std::shared_ptr<arrow::ArrayData>& data) {
  if (!data) return 0;

  int64_t total_size = 0;
  for (const auto& buffer : data->buffers) {
    if (buffer) total_size += static_cast<int64_t>(buffer->size());
  }
  for (const auto& child : data->child_data) {
    total_size += GetArrayDataSize(child);
  }
  if (data->dictionary) {
    total_size += GetArrayDataSize(data->dictionary);
  }

  return total_size;
}

int64_t GetRecordBatchSizeBytes(const std::shared_ptr<arrow::RecordBatch>& batch) {
  if (!batch) return 0;

  int64_t total_size = 0;
  for (int i = 0; i < batch->num_columns(); ++i) {
    total_size += GetArrayDataSize(batch->column(i)->data());
  }
  return total_size;
}

bool IsDetachInstrumentationDb(const std::string& sql, const std::string& instrumentation_catalog = "") {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  // Case-insensitive check for DETACH commands targeting instrumentation DB
  std::string upper = boost::to_upper_copy(trimmed);
  if (upper.find("DETACH") == std::string::npos) {
    return false;
  }
  // Check for default instrumentation DB aliases/names
  if (upper.find("_GIZMOSQL_INSTR") != std::string::npos ||
      upper.find("GIZMOSQL_INSTRUMENTATION") != std::string::npos) {
    return true;
  }
  // Check for external instrumentation catalog name (e.g., DuckLake catalog)
  if (!instrumentation_catalog.empty()) {
    std::string upper_catalog = boost::to_upper_copy(instrumentation_catalog);
    if (upper.find(upper_catalog) != std::string::npos) {
      return true;
    }
  }
  return false;
}

// Generic DETACH guard: true if `sql` is a DETACH that mentions `catalog`
// (same substring heuristic as IsDetachInstrumentationDb). Used to also protect
// the catalog-logging catalog from being detached out from under the sink.
bool SqlDetachesCatalog(const std::string& sql, const std::string& catalog) {
  if (catalog.empty()) return false;
  std::string upper = boost::to_upper_copy(sql);
  if (upper.find("DETACH") == std::string::npos) {
    return false;
  }
  return upper.find(boost::to_upper_copy(catalog)) != std::string::npos;
}

#ifndef GIZMOSQL_ENTERPRISE
// Core edition: local implementation for detection only
bool IsKillSessionCommand(const std::string& sql, std::string& target_session_id) {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  // Match: KILL SESSION 'uuid' or KILL SESSION "uuid" or KILL SESSION uuid
  std::regex kill_pattern(R"(^\s*KILL\s+SESSION\s+['\"]?([0-9a-fA-F-]+)['\"]?\s*;?\s*$)",
                          std::regex_constants::icase);
  std::smatch match;
  if (std::regex_match(trimmed, match, kill_pattern)) {
    target_session_id = match[1].str();
    return true;
  }
  return false;
}
#endif

// These helpers are used by both Core and Enterprise editions
std::string UnquoteIdentifier(const std::string& identifier) {
  if (identifier.size() >= 2 && identifier.front() == '"' && identifier.back() == '"') {
    std::string unquoted = identifier.substr(1, identifier.size() - 2);
    boost::replace_all(unquoted, "\"\"", "\"");
    return unquoted;
  }
  return identifier;
}

std::optional<std::string> TryExtractUseCatalogName(const std::string& sql) {
  static const std::regex use_pattern(
      R"(^\s*USE\s+((?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*))(?:\s*\.\s*((?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*)))?\s*;?\s*$)",
      std::regex_constants::icase);

  std::smatch match;
  if (!std::regex_match(sql, match, use_pattern)) {
    return std::nullopt;
  }

  const std::string first_identifier = UnquoteIdentifier(match[1].str());
  if (match[2].matched) {
    return first_identifier;
  }

  return first_identifier;
}


// Replace GizmoSQL pseudo-functions with actual values:
//   GIZMOSQL_CURRENT_SESSION() -> current session UUID
//   GIZMOSQL_CURRENT_INSTANCE() -> current server instance UUID
//   GIZMOSQL_CURRENT_CLUSTER() -> current cluster UUID (NULL if --cluster-id unset)
//   GIZMOSQL_VERSION() -> GizmoSQL version string
//   GIZMOSQL_USER() -> current username
//   GIZMOSQL_ROLE() -> current user's role
//   GIZMOSQL_EDITION() -> edition name ("Core" or "Enterprise")
// Only replaces occurrences that are NOT within quoted strings.
// When in a SELECT expression context, adds an alias matching the function name.

// Check if we should add an alias based on context (what precedes and follows).
// Returns true if in a SELECT expression context, false if in a condition context.
bool ShouldAddAlias(const std::string& sql, size_t func_start, size_t func_end) {
  // First check what precedes the function - if preceded by comparison operator, don't alias
  if (func_start > 0) {
    size_t prev_pos = func_start - 1;
    // Skip whitespace backwards
    while (prev_pos > 0 && std::isspace(sql[prev_pos])) {
      prev_pos--;
    }

    if (prev_pos < sql.size()) {
      char prev_char = sql[prev_pos];
      // If preceded by comparison operators, we're in a condition context
      if (prev_char == '=' || prev_char == '<' || prev_char == '>' || prev_char == '!') {
        return false;
      }
      // Check for multi-char operators like !=, <>, <=, >=
      if (prev_pos > 0) {
        std::string two_char = sql.substr(prev_pos - 1, 2);
        if (two_char == "!=" || two_char == "<>" || two_char == "<=" || two_char == ">=") {
          return false;
        }
      }
    }
  }

  // Check what follows the function
  size_t pos = func_end;
  // Skip whitespace
  while (pos < sql.size() && std::isspace(sql[pos])) {
    pos++;
  }

  if (pos >= sql.size()) {
    return true;  // End of string - likely SELECT expression
  }

  char next_char = sql[pos];

  // These characters indicate end of a SELECT expression
  if (next_char == ',' || next_char == ';') {
    return true;
  }

  // Closing paren could be either context - check what follows it
  if (next_char == ')') {
    return true;  // Typically fine to alias inside parens in SELECT
  }

  // Check for user-provided AS alias
  std::string rest = sql.substr(pos);
  std::string upper_rest = boost::to_upper_copy(rest);
  if (upper_rest.rfind("AS ", 0) == 0 || upper_rest.rfind("AS\t", 0) == 0 ||
      upper_rest.rfind("AS\n", 0) == 0 || upper_rest.rfind("AS\r", 0) == 0) {
    return false;  // User providing their own alias
  }

  // Check for SQL keywords that follow SELECT expressions
  static const std::vector<std::string> select_terminators = {
      "FROM ", "FROM\t", "FROM\n", "FROM\r",
      "WHERE ", "WHERE\t", "WHERE\n", "WHERE\r",
      "ORDER ", "ORDER\t", "ORDER\n", "ORDER\r",
      "GROUP ", "GROUP\t", "GROUP\n", "GROUP\r",
      "HAVING ", "HAVING\t", "HAVING\n", "HAVING\r",
      "LIMIT ", "LIMIT\t", "LIMIT\n", "LIMIT\r",
      "UNION ", "UNION\t", "UNION\n", "UNION\r",
      "EXCEPT ", "EXCEPT\t", "EXCEPT\n", "EXCEPT\r",
      "INTERSECT ", "INTERSECT\t", "INTERSECT\n", "INTERSECT\r",
  };

  for (const auto& terminator : select_terminators) {
    if (upper_rest.rfind(terminator, 0) == 0) {
      return true;
    }
  }

  // Check for condition keywords/operators that indicate we're NOT in a SELECT expression
  static const std::vector<std::string> condition_indicators = {
      "AND ", "AND\t", "AND\n", "AND\r",
      "OR ", "OR\t", "OR\n", "OR\r",
      "IN ", "IN\t", "IN\n", "IN\r", "IN(",
      "LIKE ", "LIKE\t", "LIKE\n", "LIKE\r",
      "BETWEEN ", "BETWEEN\t", "BETWEEN\n", "BETWEEN\r",
      "IS ", "IS\t", "IS\n", "IS\r",
  };

  for (const auto& indicator : condition_indicators) {
    if (upper_rest.rfind(indicator, 0) == 0) {
      return false;
    }
  }

  // If followed by comparison operators, we're in a condition
  if (next_char == '=' || next_char == '<' || next_char == '>' || next_char == '!') {
    return false;
  }

  // Default: don't add alias to be safe
  return false;
}

// Statement handles and execution ids come from one random_generator per thread
// instead of one constructed, and seeded from the OS, per call; the text is the same random
// v4 UUID (kThreadLocalUuid; off, one generator per call).
bool ThreadLocalUuidEnabled() {
  return gizmosql::kThreadLocalUuid;
}

std::string NewUuid() {
  if (!ThreadLocalUuidEnabled()) {
    return boost::uuids::to_string(boost::uuids::random_generator()());
  }
  thread_local boost::uuids::random_generator generator;
  return boost::uuids::to_string(generator());
}

// Every token ReplaceGizmoSQLFunctions replaces begins with GIZMOSQL_, its
// comparisons run under the classic locale (whose toupper maps a-z alone), and outside a
// match it copies every byte, so a text without that prefix (ASCII case-insensitive,
// quoted or not) comes back unchanged (kStatementTextFastPaths; off, the loop always runs).
bool ReplaceFastPathEnabled() {
  return gizmosql::kStatementTextFastPaths;
}

bool ContainsGizmoSQLPrefix(const std::string& sql) {
  static constexpr char kPrefix[] = "gizmosql_";
  constexpr size_t kLen = sizeof(kPrefix) - 1;
  for (size_t i = 0; i + kLen <= sql.size(); ++i) {
    size_t j = 0;
    while (j + 1 < kLen && (sql[i + j] | 0x20) == kPrefix[j]) {
      ++j;
    }
    if (j + 1 == kLen && sql[i + j] == '_') {
      return true;
    }
  }
  return false;
}

std::string ReplaceGizmoSQLFunctions(const std::string& sql,
                                     const std::string& session_id,
                                     const std::string& instance_id,
                                     const std::string& cluster_id,
                                     const std::string& username,
                                     const std::string& role,
                                     const std::string& edition,
                                     bool instrumentation_enabled,
                                     const std::string& instrumentation_catalog,
                                     const std::string& instrumentation_schema) {
  if (ReplaceFastPathEnabled() && !ContainsGizmoSQLPrefix(sql)) {
    return sql;
  }
  std::string result;
  result.reserve(sql.size() * 2);  // Extra space for potential aliases

  size_t i = 0;
  while (i < sql.size()) {
    // Skip single-quoted strings
    if (sql[i] == '\'') {
      result += sql[i++];
      while (i < sql.size() && sql[i] != '\'') {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          result += sql[i++];
        }
        if (i < sql.size()) {
          result += sql[i++];
        }
      }
      if (i < sql.size()) {
        result += sql[i++];  // closing quote
      }
      continue;
    }

    // Skip double-quoted strings (identifiers in DuckDB)
    if (sql[i] == '"') {
      result += sql[i++];
      while (i < sql.size() && sql[i] != '"') {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          result += sql[i++];
        }
        if (i < sql.size()) {
          result += sql[i++];
        }
      }
      if (i < sql.size()) {
        result += sql[i++];  // closing quote
      }
      continue;
    }

    // Check for GIZMOSQL_CURRENT_SESSION() at this position
    constexpr size_t kSessionFuncLen = 26;  // length of "GIZMOSQL_CURRENT_SESSION()"
    if (i + kSessionFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kSessionFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_CURRENT_SESSION()") {
        result += '\'';
        result += session_id;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kSessionFuncLen)) {
          result += " AS \"GIZMOSQL_CURRENT_SESSION()\"";
        }
        i += kSessionFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_CURRENT_INSTANCE() at this position
    constexpr size_t kInstanceFuncLen = 27;  // length of "GIZMOSQL_CURRENT_INSTANCE()"
    if (i + kInstanceFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstanceFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_CURRENT_INSTANCE()") {
        if (instance_id.empty()) {
          result += "NULL";
        } else {
          result += '\'';
          result += instance_id;
          result += '\'';
        }
        if (ShouldAddAlias(sql, i, i + kInstanceFuncLen)) {
          result += " AS \"GIZMOSQL_CURRENT_INSTANCE()\"";
        }
        i += kInstanceFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_CURRENT_CLUSTER() at this position
    constexpr size_t kClusterFuncLen = 26;  // length of "GIZMOSQL_CURRENT_CLUSTER()"
    if (i + kClusterFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kClusterFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_CURRENT_CLUSTER()") {
        if (cluster_id.empty()) {
          result += "NULL";
        } else {
          result += '\'';
          result += cluster_id;
          result += '\'';
        }
        if (ShouldAddAlias(sql, i, i + kClusterFuncLen)) {
          result += " AS \"GIZMOSQL_CURRENT_CLUSTER()\"";
        }
        i += kClusterFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_VERSION() at this position
    constexpr size_t kVersionFuncLen = 18;  // length of "GIZMOSQL_VERSION()"
    if (i + kVersionFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kVersionFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_VERSION()") {
        result += '\'';
        // Use the channel-aware version string so SELECT GIZMOSQL_VERSION()
        // on an LTS server returns e.g. "v1.25.0-LTS" rather than the bare
        // git tag "v1.25.0". Stable builds remain unchanged.
        result += GIZMOSQL_SERVER_VERSION;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kVersionFuncLen)) {
          result += " AS \"GIZMOSQL_VERSION()\"";
        }
        i += kVersionFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_USER() at this position
    constexpr size_t kUserFuncLen = 15;  // length of "GIZMOSQL_USER()"
    if (i + kUserFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kUserFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_USER()") {
        result += '\'';
        result += username;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kUserFuncLen)) {
          result += " AS \"GIZMOSQL_USER()\"";
        }
        i += kUserFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_ROLE() at this position
    constexpr size_t kRoleFuncLen = 15;  // length of "GIZMOSQL_ROLE()"
    if (i + kRoleFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kRoleFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_ROLE()") {
        result += '\'';
        result += role;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kRoleFuncLen)) {
          result += " AS \"GIZMOSQL_ROLE()\"";
        }
        i += kRoleFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_EDITION() at this position
    constexpr size_t kEditionFuncLen = 18;  // length of "GIZMOSQL_EDITION()"
    if (i + kEditionFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kEditionFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_EDITION()") {
        result += '\'';
        result += edition;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kEditionFuncLen)) {
          result += " AS \"GIZMOSQL_EDITION()\"";
        }
        i += kEditionFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_INSTRUMENTATION_ENABLED() at this position
    constexpr size_t kInstrEnabledFuncLen = 34;  // length of "GIZMOSQL_INSTRUMENTATION_ENABLED()"
    if (i + kInstrEnabledFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstrEnabledFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_INSTRUMENTATION_ENABLED()") {
        result += instrumentation_enabled ? "true" : "false";
        if (ShouldAddAlias(sql, i, i + kInstrEnabledFuncLen)) {
          result += " AS \"GIZMOSQL_INSTRUMENTATION_ENABLED()\"";
        }
        i += kInstrEnabledFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_INSTRUMENTATION_CATALOG() at this position
    constexpr size_t kInstrCatalogFuncLen = 34;  // length of "GIZMOSQL_INSTRUMENTATION_CATALOG()"
    if (i + kInstrCatalogFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstrCatalogFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_INSTRUMENTATION_CATALOG()") {
        result += '\'';
        result += instrumentation_catalog;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kInstrCatalogFuncLen)) {
          result += " AS \"GIZMOSQL_INSTRUMENTATION_CATALOG()\"";
        }
        i += kInstrCatalogFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_INSTRUMENTATION_SCHEMA() at this position
    constexpr size_t kInstrSchemaFuncLen = 33;  // length of "GIZMOSQL_INSTRUMENTATION_SCHEMA()"
    if (i + kInstrSchemaFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstrSchemaFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_INSTRUMENTATION_SCHEMA()") {
        result += '\'';
        result += instrumentation_schema;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kInstrSchemaFuncLen)) {
          result += " AS \"GIZMOSQL_INSTRUMENTATION_SCHEMA()\"";
        }
        i += kInstrSchemaFuncLen;
        continue;
      }
    }

    result += sql[i++];
  }

  return result;
}

}  // namespace

// The vendored jemalloc's mallctl, a C symbol of the DuckDB library this server links
// (jemalloc/jemalloc.h is not on this target's include path).
extern "C" int duckdb_je_mallctl(const char* name, void* oldp, size_t* oldlenp, void* newp,
                                 size_t newlen);

namespace {

// Statements run on a persistent executor thread instead of a fresh std::async
// thread. A statement thread that exits is its jemalloc arena's last thread, and
// jemalloc's tcache_destroy then purges that arena whatever dirty_decay_ms says, so the
// next statement's thread starts on a cold arena. The executor's threads never exit
// while they are kept, so their arenas keep their pages between back-to-back statements
// as DuckDB's own workers do, and release them on the workers' idle clock.
//
// kStatementExecutorPool; off, std::async as before (no executor is created).
bool ExecPoolEnabled() {
  return gizmosql::kStatementExecutorPool;
}

// TaskScheduler::ExecuteForever's idle flush for a thread idle 0.5 s, in its
// allocator_background_threads=false form, through mallctl: past DuckDB's default
// allocator_flush_threshold (128 MiB) of peak, flush the tcache, purge this thread's
// arena and reset the peak; then trim the system heap.
void ExecIdleFlush() {
  constexpr uint64_t kFlushThreshold = 134217728ULL;
  uint64_t peak = 0;
  size_t peak_len = sizeof(peak);
  if (duckdb_je_mallctl("thread.peak.read", &peak, &peak_len, nullptr, 0) == 0 &&
      peak > kFlushThreshold) {
    duckdb_je_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    unsigned arena = 0;
    size_t arena_len = sizeof(arena);
    if (duckdb_je_mallctl("thread.arena", &arena, &arena_len, nullptr, 0) == 0) {
      char purge[48];
      std::snprintf(purge, sizeof(purge), "arena.%u.purge", arena);
      duckdb_je_mallctl(purge, nullptr, nullptr, nullptr, 0);
    }
    duckdb_je_mallctl("thread.peak.reset", nullptr, nullptr, nullptr, 0);
  }
  malloc_trim(0);
}

// Allocator::ThreadIdle, for a thread idle for DuckDB's decay delay.
void ExecThreadIdle() {
  duckdb_je_mallctl("thread.idle", nullptr, nullptr, nullptr, 0);
  duckdb_je_mallctl("thread.peak.reset", nullptr, nullptr, nullptr, 0);
}

// A process-wide executor for DuckDBStatement::Execute's statement lambda. Submit hands
// the task to the most recently idle thread, or spawns one when none is idle: there is
// no bound on threads (std::async had none), so concurrent statements never queue
// behind each other. A thread runs its task, destroys it (and with it the captured
// session), returns to the idle stack and waits; idle 0.5 s it runs the idle flush, and
// an idle thread beyond kKeepIdleMax then exits; idle DuckDB's decay delay (1 s) more it
// marks itself idle to jemalloc and waits untimed.
class StatementExecutor {
 public:
  using Task = std::packaged_task<arrow::Result<int>()>;

  // Created on first use and never destroyed: its threads live as long as the process.
  static StatementExecutor& Instance() {
    static StatementExecutor* const instance = new StatementExecutor();
    return *instance;
  }

  template <typename F>
  std::future<arrow::Result<int>> Submit(F&& fn) {
    Task task(std::forward<F>(fn));
    auto future = task.get_future();
    std::shared_ptr<Worker> worker;
    bool spawn = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (idle_.empty()) {
        worker = std::make_shared<Worker>();
        ++spawned_;
        spawn = true;
      } else {
        worker = std::move(idle_.back());
        idle_.pop_back();
      }
      worker->task = std::move(task);
      worker->stmt = ++submitted_;
      worker->has_task = true;
    }
    if (spawn) {
      std::thread(&StatementExecutor::Run, this, worker).detach();
    } else {
      worker->cv.notify_one();
    }
    return future;
  }

 private:
  struct Worker {
    std::condition_variable cv;
    Task task;
    uint64_t stmt = 0;
    bool has_task = false;
  };

  static constexpr size_t kKeepIdleMax = 16;
  static constexpr auto kIdleFlushWait = std::chrono::milliseconds(500);
  static constexpr auto kDecayDelay = std::chrono::seconds(1);

  StatementExecutor() = default;

  void Run(std::shared_ptr<Worker> self) {
    const auto has_task = [&self] { return self->has_task; };
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
      {
        Task task = std::move(self->task);
        self->has_task = false;
        lock.unlock();
        task();
      }  // the finished task is destroyed before this thread idles
      lock.lock();
      idle_.push_back(self);
      if (self->cv.wait_for(lock, kIdleFlushWait, has_task)) {
        continue;
      }
      lock.unlock();
      ExecIdleFlush();
      lock.lock();
      ++flushes_;
      if (!self->has_task && idle_.size() > kKeepIdleMax) {
        for (auto it = idle_.begin(); it != idle_.end(); ++it) {
          if (*it == self) {
            idle_.erase(it);
            break;
          }
        }
        return;
      }
      if (self->cv.wait_for(lock, kDecayDelay, has_task)) {
        continue;
      }
      lock.unlock();
      ExecThreadIdle();
      lock.lock();
      self->cv.wait(lock, has_task);
    }
  }

  std::mutex mutex_;
  std::vector<std::shared_ptr<Worker>> idle_;  // the idle stack, most recently idle last
  uint64_t submitted_ = 0;
  uint64_t spawned_ = 0;
  uint64_t flushes_ = 0;
  long first_tid_ = 0;
};

// the executor, or std::async when the executor is off
template <typename F>
std::future<arrow::Result<int>> LaunchStatement(std::launch policy, F&& fn) {
  if (!ExecPoolEnabled()) {
    return std::async(policy, std::forward<F>(fn));
  }
  return StatementExecutor::Instance().Submit(std::forward<F>(fn));
}

// std::async's future waits for its task in its destructor and a packaged_task's does
// not: with the executor enabled, wait for the task if Execute unwinds before its own
// waits, so the statement never runs past Execute's return.
struct WaitOnUnwind {
  std::future<arrow::Result<int>>& future;
  ~WaitOnUnwind() {
    if (ExecPoolEnabled() && future.valid()) {
      future.wait();
    }
  }
};

}  // namespace

namespace gizmosql::ddb {

// Resolve whether a catalog with the given name is attached.
//
// Deliberately uses duckdb_databases() rather than information_schema.schemata:
// the latter enumerates the schemas of EVERY attached catalog, and for remote
// catalogs (DuckLake on PostgreSQL, postgres_scanner, ...) that opens one
// metadata-store connection per attached catalog even though the predicate
// names exactly one. duckdb_databases() is answered from the in-process
// DatabaseManager and never touches a remote store.
bool CatalogExistsOnConnection(duckdb::Connection& connection,
                               const std::string& catalog_name) {
  auto stmt = connection.Prepare(
      "SELECT 1 FROM duckdb_databases() WHERE database_name = ? LIMIT 1");
  if (!stmt || !stmt->success) {
    return false;
  }

  duckdb::vector<duckdb::Value> bind_parameters;
  bind_parameters.emplace_back(catalog_name);
  auto result = stmt->Execute(bind_parameters);
  if (!result || result->HasError()) {
    return false;
  }

  auto row = result->Fetch();
  return row != nullptr && row->size() > 0;
}

// A process-wide, bounded, leased cache of DuckDB PreparedStatementData, consulted
// by the DoGetStatement and GetFlightInfoStatement Creates of client sessions and
// keyed by the statement bytes after GizmoSQL's rewrites and the planning
// environment. Only a statement passing four tiers is cacheable: the raw-text
// denylist, the statement test, the narrowing tests and the operator allowlist walked
// over the prepared statement's own physical plan; every execution of any other statement, on
// every entry point, is bracketed by a pair of generation bumps (the in-flight
// count between them hides every entry). With kPlanCache off the path is unchanged: no
// store and no generation counter. The key carries the engine's dictionary
// publication version (kPlanCacheDictionaryVersion).
bool PlanCacheEnabled();
void PlanCachePairBegin();
void PlanCachePairEnd();
void PlanCacheFlagSession(const std::string& session_id);

namespace {

constexpr size_t kPlanCacheEntries = 256;

// The engine types a group key as a dictionary code only on a dictionary published when the plan
// is made (PlanCodeKeys), so the key carries dict_global::PublicationVersion(),
// read before the lookup and the miss's Prepare: a plan made before a publish (the
// first execution builds the dictionary) is never served after it, and a plan
// that typed a dictionary never once that dictionary stops being published.
bool PlanCacheDictVersionEnabled() {
  return gizmosql::kPlanCacheDictionaryVersion;
}

// Tier 0's raw-text denylist and the walker's function-name denylist: `x*` a prefix,
// else a whole word; identifier characters end a token on both sides; case-insensitive.
// It stays beside the stability test: a call folded at optimization, replaced at bind by
// session state or nested in a lambda body never reaches the walker as a function; and
// one-argument age() is declared CONSISTENT but reads the transaction's date.
constexpr const char* kPlanCacheDenylist[] = {
    "current_*",        "getvariable",   "setseed",       "random",
    "uuid*",            "gen_random_uuid", "nextval",     "currval",
    "now",              "today",         "transaction_timestamp", "localtime*",
    "get_current_time*", "txid_current", "in_search_path", "list_sort",
    "list_reverse_sort", "array_sort",   "array_reverse_sort", "list_grade_up",
    "write_log",        "constant_or_null", "pg_*",       "version",
    "age"};

bool IsIdentifierChar(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
         c == '_';
}

bool DenylistedWord(std::string_view word) {
  std::string lower(word);
  for (auto& c : lower) {
    if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
  }
  for (std::string_view entry : kPlanCacheDenylist) {
    if (entry.back() == '*') {
      if (lower.compare(0, entry.size() - 1, entry.substr(0, entry.size() - 1)) == 0 &&
          lower.size() >= entry.size() - 1) {
        return true;
      }
    } else if (lower == entry) {
      return true;
    }
  }
  return false;
}

// Tier 0: any whole-word denylist token anywhere in the text, literals included.
bool TextDenylisted(const std::string& sql) {
  size_t i = 0;
  while (i < sql.size()) {
    if (!IsIdentifierChar(sql[i])) {
      ++i;
      continue;
    }
    size_t j = i;
    while (j < sql.size() && IsIdentifierChar(sql[j])) ++j;
    if (DenylistedWord(std::string_view(sql).substr(i, j - i))) return true;
    i = j;
  }
  return false;
}

std::string Sha256Hex(const std::string& text) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int length = 0;
  if (EVP_Digest(text.data(), text.size(), md, &length, EVP_sha256(), nullptr) != 1) {
    return std::string(64, '0');
  }
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(length * 2);
  for (unsigned int i = 0; i < length; i++) {
    out += kHex[md[i] >> 4];
    out += kHex[md[i] & 15];
  }
  return out;
}

struct PlanEntry {
  const duckdb::DatabaseInstance* db = nullptr;
  std::string key;
  duckdb::shared_ptr<duckdb::PreparedStatementData> data;
  std::string query;  // the text PrepareInternal stored
  std::string username;
  std::string role;
  uint64_t generation = 0;  // the counter read at lookup, before the miss's Prepare
  bool leased = false;
  bool linked = false;
};

struct InstancePlans {
  std::string primary;  // the primary catalog's name, read from a fresh connection
  bool primary_known = false;
  std::list<std::shared_ptr<PlanEntry>> lru;  // front: the most recently used
  std::unordered_map<std::string, std::list<std::shared_ptr<PlanEntry>>::iterator> map;
};

struct SessionPlanRecord {
  bool flagged = false;  // ran a statement failing the statement test since its creation
  bool snapshot_read = false;
  std::string snapshot;  // SHA-256 (hex) of the settings snapshot
};

struct StatementPlanRecord {
  bool cacheable = false;
  uint64_t stmt = 0;
  std::shared_ptr<PlanEntry> lease;
};

struct PlanCache {
  std::mutex mutex;
  uint64_t generation = 0;
  int64_t inflight = 0;
  std::unordered_map<const duckdb::DatabaseInstance*, InstancePlans> instances;
  std::unordered_map<std::string, SessionPlanRecord> sessions;
  std::unordered_map<const DuckDBStatement*, StatementPlanRecord> statements;
  std::unordered_map<std::string, bool> server_texts;  // a server-own text -> it pairs
  std::atomic<uint64_t> next_stmt{0};
};

// Never destroyed, as it may be used until exit.
PlanCache& GetPlanCache() {
  static auto* cache = new PlanCache();
  return *cache;
}

void UnlinkEntry(InstancePlans& plans, PlanEntry& entry) {
  if (!entry.linked) return;
  // cleared before the erase: the list node may hold the entry's last reference
  entry.linked = false;
  auto found = plans.map.find(entry.key);
  if (found != plans.map.end()) {
    plans.lru.erase(found->second);
    plans.map.erase(found);
  }
}

// The settings snapshot: session-scoped state only (global state - DBConfig, extension defaults, SET
// GLOBAL, LOAD, INSTALL - moves the generation instead), read by member access: the
// ClientConfig fields the legacy LOCAL settings and the PRAGMAs write and the SET
// VARIABLE values, sorted by name. A LOCAL override of a generic option (an extension
// option's included) sits in ClientConfig::user_settings, whose storage is private in the
// embedded DuckDB; the session flag is its guard: every SET and RESET is a SET_STATEMENT and
// every PRAGMA a PRAGMA_STATEMENT, so each fails the statement test and flags the session.
std::string ReadSettingsSnapshot(duckdb::ClientContext& context) {
  std::vector<std::pair<std::string, std::string>> rows;
  const auto& config = context.config;
  auto field = [&rows](const char* name, int64_t value) {
    rows.emplace_back(name, std::to_string(value));
  };
  field("enable_profiler", config.enable_profiler);
  field("enable_detailed_profiling", config.enable_detailed_profiling);
  field("profiler_print_format", static_cast<int64_t>(config.profiler_print_format));
  rows.emplace_back("profiler_save_location", config.profiler_save_location);
  std::vector<int64_t> metrics;
  for (auto metric : config.profiler_settings) metrics.push_back(static_cast<int64_t>(metric));
  std::sort(metrics.begin(), metrics.end());
  std::string metric_text;
  for (auto metric : metrics) metric_text += std::to_string(metric) + ",";
  rows.emplace_back("profiler_settings", metric_text);
  field("profiler_settings_type", static_cast<int64_t>(config.profiler_settings_type));
  field("emit_profiler_output", config.emit_profiler_output);
  rows.emplace_back("system_progress_bar_disable_reason",
                    config.system_progress_bar_disable_reason
                        ? config.system_progress_bar_disable_reason
                        : "");
  field("enable_progress_bar", config.enable_progress_bar);
  field("print_progress_bar", config.print_progress_bar);
  field("wait_time", config.wait_time);
  field("query_verification_enabled", config.query_verification_enabled);
  field("verify_external", config.verify_external);
  field("verify_fetch_row", config.verify_fetch_row);
  field("verify_serializer", config.verify_serializer);
  field("enable_optimizer", config.enable_optimizer);
  field("enable_caching_operators", config.enable_caching_operators);
  field("verify_parallelism", config.verify_parallelism);
  field("force_external", config.force_external);
  field("force_fetch_row", config.force_fetch_row);
  field("use_replacement_scans", config.use_replacement_scans);
  field("streaming_buffer_size", static_cast<int64_t>(config.streaming_buffer_size));
  field("profiling_coverage", static_cast<int64_t>(config.profiling_coverage));
  field("enable_http_logging", config.enable_http_logging);
  rows.emplace_back("http_logging_output", config.http_logging_output);
  for (auto& variable : config.user_variables) {
    rows.emplace_back("variable:" + variable.first, variable.second.ToString());
  }
  std::sort(rows.begin(), rows.end());
  std::string text;
  for (auto& row : rows) {
    text += std::to_string(row.first.size()) + ":" + row.first +
            std::to_string(row.second.size()) + ":" + row.second;
  }
  return Sha256Hex(text);
}

// The key: the statement bytes, the search path, the user, the role, the
// catalog access rules and the settings snapshot (the generation is the entry's tag).
std::string PlanKey(const std::string& sql, duckdb::ClientContext& context,
                    const ClientSession& session, const std::string& snapshot) {
  std::string key;
  auto add = [&key](const std::string& part) {
    key += std::to_string(part.size());
    key += ':';
    key += part;
  };
  add(sql);
  add(duckdb::CatalogSearchEntry::ListToString(
      duckdb::ClientData::Get(context).catalog_search_path->GetSetPaths()));
  add(session.username);
  add(session.role);
  std::string access;
  for (const auto& rule : session.catalog_access) {
    access += rule.catalog;
    access += '\x1f';
    access += std::to_string(static_cast<int>(rule.access));
    access += '\x1e';
  }
  add(access);
  add(snapshot);
  if (PlanCacheDictVersionEnabled()) {
    add(std::to_string(duckdb::dict_global::PublicationVersion()));
  }
  return key;
}

// The statement test on a prepared statement: nullptr when it passes, else the failing test.
const char* StatementTest(const duckdb::PreparedStatement& stmt, bool in_transaction,
                          const std::string& primary, bool primary_known) {
  if (!stmt.success || !stmt.data) return "single";
  if (stmt.data->statement_type != duckdb::StatementType::SELECT_STATEMENT) return "type";
  if (!stmt.data->properties.IsReadOnly()) return "writes";
  for (const auto& database : stmt.data->properties.read_databases) {
    if (!primary_known || database.first != primary) return "database";
  }
  if (in_transaction) return "transaction";
  return nullptr;
}

// The operator allowlist, walked over the prepared statement's own physical plan - the artifact the cache
// serves - with the logical allowlist mapped onto the physical operators the plan
// generator makes of each allowed logical operator: every
// operator, every operator-owned expression and every table filter; anything unmapped
// fails closed. The class is a LogicalOperatorVisitor only for its expression recursion.
class AllowlistWalker : public duckdb::LogicalOperatorVisitor {
 public:
  enum Test : unsigned {
    kOperator = 1,
    kTable = 2,
    kFilter = 4,
    kExpression = 8,
    kFunction = 16,
    kAggregate = 32,
    kPlan = 64
  };

  explicit AllowlistWalker(const std::string& primary) : primary_(primary) {}

  unsigned failed = 0;

  void VisitExpression(duckdb::unique_ptr<duckdb::Expression>* expression) override {
    if (expression == nullptr || !*expression) {
      failed |= kPlan;
      return;
    }
    auto& expr = **expression;
    switch (expr.GetExpressionClass()) {
      case duckdb::ExpressionClass::BOUND_COLUMN_REF:
      case duckdb::ExpressionClass::BOUND_REF:
      case duckdb::ExpressionClass::BOUND_CONSTANT:
      case duckdb::ExpressionClass::BOUND_COMPARISON:
      case duckdb::ExpressionClass::BOUND_CONJUNCTION:
      case duckdb::ExpressionClass::BOUND_OPERATOR:
      case duckdb::ExpressionClass::BOUND_CAST:
      case duckdb::ExpressionClass::BOUND_BETWEEN:
      case duckdb::ExpressionClass::BOUND_CASE:
        break;
      case duckdb::ExpressionClass::BOUND_FUNCTION: {
        auto& function = static_cast<duckdb::BoundFunctionExpression&>(expr).function;
        CheckFunction(function.name, function.GetStability());
        break;
      }
      case duckdb::ExpressionClass::BOUND_AGGREGATE: {
        auto& aggregate = static_cast<duckdb::BoundAggregateExpression&>(expr);
        CheckFunction(aggregate.function.name, aggregate.function.GetStability());
        // physical planning replaces an ordered aggregate by a same-named wrapper whose
        // bind data (it keeps a ClientContext &) its own bind callback did not make
        if (aggregate.order_bys || aggregate.filter ||
            (aggregate.bind_info && !aggregate.function.bind)) {
          failed |= kAggregate;
        }
        break;
      }
      default:
        failed |= kExpression;
        return;
    }
    VisitExpressionChildren(expr);
  }

  void Walk(duckdb::PhysicalOperator& root) {
    std::vector<duckdb::PhysicalOperator*> stack{&root};
    std::unordered_set<duckdb::PhysicalOperator*> seen;
    while (!stack.empty()) {
      auto* op = stack.back();
      stack.pop_back();
      if (!seen.insert(op).second) continue;
      VisitPhysical(*op);
      for (auto& child : op->GetChildren()) {
        stack.push_back(&const_cast<duckdb::PhysicalOperator&>(child.get()));
      }
    }
  }

 private:
  // a bound scalar or aggregate function: DuckDB declares it CONSISTENT, no denylist word
  void CheckFunction(const std::string& name, duckdb::FunctionStability stability) {
    if (stability != duckdb::FunctionStability::CONSISTENT || DenylistedWord(name)) {
      failed |= kFunction;
    }
  }

  void Visit(duckdb::unique_ptr<duckdb::Expression>& expression) { VisitExpression(&expression); }

  void Visit(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& expressions) {
    for (auto& expression : expressions) Visit(expression);
  }

  void Visit(duckdb::vector<duckdb::BoundOrderByNode>& orders) {
    for (auto& order : orders) Visit(order.expression);
  }

  void Visit(duckdb::BoundLimitNode& limit) {
    if (limit.GetExpression()) Visit(limit.GetExpression());
  }

  void VisitConditions(duckdb::PhysicalComparisonJoin& join) {
    for (auto& condition : join.conditions) {
      Visit(condition.left);
      Visit(condition.right);
    }
  }

  void VisitPhysical(duckdb::PhysicalOperator& op) {
    using T = duckdb::PhysicalOperatorType;
    switch (op.type) {
      case T::TABLE_SCAN:  // LOGICAL_GET
        VisitScan(static_cast<duckdb::PhysicalTableScan&>(op));
        return;
      case T::FILTER:  // LOGICAL_FILTER (and a LOGICAL_GET's remaining filter)
        Visit(static_cast<duckdb::PhysicalFilter&>(op).expression);
        return;
      case T::PROJECTION:  // LOGICAL_PROJECTION (and the generator's own projections)
        Visit(static_cast<duckdb::PhysicalProjection&>(op).select_list);
        return;
      case T::UNGROUPED_AGGREGATE:
        Visit(static_cast<duckdb::PhysicalUngroupedAggregate&>(op).aggregates);
        return;
      case T::HASH_GROUP_BY: {
        auto& data = static_cast<duckdb::PhysicalHashAggregate&>(op).grouped_aggregate_data;
        Visit(data.groups);
        Visit(data.aggregates);
        return;
      }
      case T::PERFECT_HASH_GROUP_BY: {  // LOGICAL_AGGREGATE_AND_GROUP_BY
        auto& aggregate = static_cast<duckdb::PhysicalPerfectHashAggregate&>(op);
        Visit(aggregate.groups);
        Visit(aggregate.aggregates);
        return;
      }
      case T::PARTITIONED_AGGREGATE: {  // LOGICAL_AGGREGATE_AND_GROUP_BY
        auto& aggregate = static_cast<duckdb::PhysicalPartitionedAggregate&>(op);
        Visit(aggregate.groups);
        Visit(aggregate.aggregates);
        return;
      }
      case T::STREAMING_FIRST_KEYS:
        Visit(static_cast<duckdb::PhysicalStreamingFirstKeys&>(op).groups);
        return;
      case T::ORDER_BY:  // LOGICAL_ORDER_BY
        Visit(static_cast<duckdb::PhysicalOrder&>(op).orders);
        return;
      case T::TOP_N:  // LOGICAL_TOP_N
        Visit(static_cast<duckdb::PhysicalTopN&>(op).orders);
        return;
      case T::LIMIT: {  // LOGICAL_LIMIT
        auto& limit = static_cast<duckdb::PhysicalLimit&>(op);
        Visit(limit.limit_val);
        Visit(limit.offset_val);
        return;
      }
      case T::STREAMING_LIMIT: {  // LOGICAL_LIMIT
        auto& limit = static_cast<duckdb::PhysicalStreamingLimit&>(op);
        Visit(limit.limit_val);
        Visit(limit.offset_val);
        return;
      }
      case T::LIMIT_PERCENT: {  // LOGICAL_LIMIT
        auto& limit = static_cast<duckdb::PhysicalLimitPercent&>(op);
        Visit(limit.limit_val);
        Visit(limit.offset_val);
        return;
      }
      case T::HASH_JOIN:  // LOGICAL_COMPARISON_JOIN
        VisitConditions(static_cast<duckdb::PhysicalComparisonJoin&>(op));
        return;
      case T::NESTED_LOOP_JOIN: {  // LOGICAL_COMPARISON_JOIN
        auto& join = static_cast<duckdb::PhysicalNestedLoopJoin&>(op);
        VisitConditions(join);
        if (join.predicate) Visit(join.predicate);
        return;
      }
      case T::PIECEWISE_MERGE_JOIN: {  // LOGICAL_COMPARISON_JOIN
        auto& join = static_cast<duckdb::PhysicalPiecewiseMergeJoin&>(op);
        VisitConditions(join);
        Visit(join.lhs_orders);
        Visit(join.rhs_orders);
        return;
      }
      case T::IE_JOIN: {  // LOGICAL_COMPARISON_JOIN
        auto& join = static_cast<duckdb::PhysicalIEJoin&>(op);
        VisitConditions(join);
        Visit(join.lhs_orders);
        Visit(join.rhs_orders);
        return;
      }
      default:  // unmapped: BLOCKWISE_NL_JOIN, CROSS_PRODUCT, INOUT_FUNCTION and every other
        failed |= kOperator;
        return;
    }
  }

  // seq_scan over a non-temporary table of the primary catalog, the catalog neither
  // system nor temporary, with table filters of the listed kinds
  void VisitScan(duckdb::PhysicalTableScan& scan) {
    bool table_ok = scan.function.name == "seq_scan" && scan.bind_data != nullptr;
    if (table_ok) {
      auto& table = static_cast<duckdb::TableScanBindData&>(*scan.bind_data).table;
      auto& catalog = table.ParentCatalog();
      table_ok = !table.temporary && catalog.GetName() == primary_ &&
                 !catalog.IsSystemCatalog() && !catalog.IsTemporaryCatalog();
    }
    if (!table_ok) failed |= kTable;
    if (scan.table_filters) {
      for (auto& entry : scan.table_filters->filters) {
        VisitFilter(entry.second.get());
      }
    }
  }

  void VisitFilter(duckdb::TableFilter* filter) {
    if (filter == nullptr) {
      failed |= kPlan;
      return;
    }
    switch (filter->filter_type) {
      case duckdb::TableFilterType::CONSTANT_COMPARISON:
      case duckdb::TableFilterType::IS_NULL:
      case duckdb::TableFilterType::IS_NOT_NULL:
      case duckdb::TableFilterType::IN_FILTER:
      case duckdb::TableFilterType::DYNAMIC_FILTER:
        return;
      case duckdb::TableFilterType::CONJUNCTION_AND:
      case duckdb::TableFilterType::CONJUNCTION_OR:
        for (auto& child : static_cast<duckdb::ConjunctionFilter*>(filter)->child_filters) {
          VisitFilter(child.get());
        }
        return;
      case duckdb::TableFilterType::OPTIONAL_FILTER:
        VisitFilter(static_cast<duckdb::OptionalFilter*>(filter)->child_filter.get());
        return;
      case duckdb::TableFilterType::EXPRESSION_FILTER:
        VisitExpression(&static_cast<duckdb::ExpressionFilter*>(filter)->expr);
        return;
      default:
        failed |= kFilter;
        return;
    }
  }

  const std::string& primary_;
};

// The allowlist on the miss path, over the prepared plan: nullptr when it passes, else the first
// failing test (the operators over the whole plan before the scans); any exception
// is `plan`.
const char* AllowlistTest(duckdb::PreparedStatementData& data, const std::string& primary) {
  try {
    if (!data.physical_plan) return "plan";
    AllowlistWalker walker(primary);
    walker.Walk(data.physical_plan->Root());
    static constexpr std::pair<unsigned, const char*> kOrder[] = {
        {AllowlistWalker::kOperator, "operator"},     {AllowlistWalker::kTable, "table"},
        {AllowlistWalker::kFilter, "filter"},         {AllowlistWalker::kExpression, "expression"},
        {AllowlistWalker::kFunction, "function"},     {AllowlistWalker::kAggregate, "aggregate"},
        {AllowlistWalker::kPlan, "plan"}};
    for (const auto& [bit, name] : kOrder) {
      if (walker.failed & bit) return name;
    }
    return nullptr;
  } catch (...) {
    return "plan";
  }
}

// Async teardown. The lease return below clears a leased plan's operator states on the
// thread that destroys the statement: the Flight DoGet handler, inside
// ~RecordBatchStream, before gRPC sends the stream's final status. Enabled
// (kAsyncTeardown), the clear moves each HASH_GROUP_BY operator's sink state out of the
// plan instead of destroying it, releases the lease as before (so the next statement can
// take the plan at once), and hands the states to one process-wide reaper thread, which
// destroys them while the handler finishes the RPC, so the result stream no longer waits
// for the aggregate states to be destroyed. The job keeps the plan entry (the states'
// operators) and the database instance (their buffer manager and task scheduler) alive
// until the states are gone; no destructor of these states touches a ClientContext, so
// the session's connection is free for its next statement meanwhile. At most
// kMaxPendingReaps jobs wait or run; beyond that the states are destroyed inline as
// before. DrainTeardownReaper() (server shutdown) waits for every job. Off, no reaper is
// created and the path is unchanged.
bool AsyncTeardownEnabled() {
  return gizmosql::kAsyncTeardown;
}

using DetachedStates = std::vector<duckdb::unique_ptr<duckdb::GlobalSinkState>>;

class TeardownReaper {
 public:
  // Created on first use and never destroyed: its thread lives as long as the process.
  static TeardownReaper& Instance() {
    static TeardownReaper* const instance = new TeardownReaper();
    return *instance;
  }

  // Takes `states` (with what keeps their referents alive) when fewer than
  // kMaxPendingReaps jobs wait or run; otherwise, or on any failure, leaves them with
  // the caller and returns false.
  bool Submit(DetachedStates& states, const std::shared_ptr<PlanEntry>& entry,
              duckdb::shared_ptr<duckdb::DatabaseInstance>& db, uint64_t stmt) noexcept {
    bool taken = false;
    try {
      std::lock_guard<std::mutex> lock(mutex_);
      if (pending_ < kMaxPendingReaps) {
        if (!started_) {
          std::thread(&TeardownReaper::Run, this).detach();
          started_ = true;
        }
        jobs_.emplace_back();  // nothing is moved before this allocation
        Job& job = jobs_.back();
        job.states = std::move(states);
        job.entry = entry;
        job.db = std::move(db);
        job.stmt = stmt;
        taken = true;
        ++pending_;
      }
    } catch (...) {
      taken = false;
    }
    if (taken) work_.notify_one();
    return taken;
  }

  // Waits until no job waits or runs.
  void Drain() {
    std::unique_lock<std::mutex> lock(mutex_);
    idle_.wait(lock, [this] { return pending_ == 0; });
    lock.unlock();
  }

 private:
  struct Job {
    DetachedStates states;
    std::shared_ptr<PlanEntry> entry;                  // the plan the states' operators live in
    duckdb::shared_ptr<duckdb::DatabaseInstance> db;  // their buffer manager and scheduler
    uint64_t stmt = 0;
  };

  static constexpr size_t kMaxPendingReaps = 4;

  TeardownReaper() = default;

  void Run() {
    pthread_setname_np(pthread_self(), "gizmosql_reaper");  // not the spawning handler's name
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
      work_.wait(lock, [this] { return !jobs_.empty(); });
      Job job = std::move(jobs_.front());
      jobs_.pop_front();
      lock.unlock();
      job.states.clear();  // before the entry: the states reference its operators
      job.entry.reset();
      job.db.reset();
      lock.lock();
      if (--pending_ == 0) idle_.notify_all();
    }
  }

  std::mutex mutex_;
  std::condition_variable work_;
  std::condition_variable idle_;
  std::deque<Job> jobs_;
  size_t pending_ = 0;  // jobs waiting or running
  bool started_ = false;
};

// The plan's operators walked through GetChildren(), every sink and operator
// state reset, and the run-aware descriptors released through their own entry points.
// With `detached` (async teardown), a HASH_GROUP_BY operator's sink state is moved there
// instead of destroyed.
void DropPlanState(duckdb::PreparedStatementData& data, DetachedStates* detached = nullptr) {
  if (!data.physical_plan) return;
  std::vector<duckdb::PhysicalOperator*> stack{&data.physical_plan->Root()};
  std::unordered_set<duckdb::PhysicalOperator*> seen;
  while (!stack.empty()) {
    auto* op = stack.back();
    stack.pop_back();
    if (!seen.insert(op).second) continue;
    for (auto& child : op->GetChildren()) {
      stack.push_back(&const_cast<duckdb::PhysicalOperator&>(child.get()));
    }
    if (op->type == duckdb::PhysicalOperatorType::HASH_GROUP_BY) {
      auto& aggregate = static_cast<duckdb::PhysicalHashAggregate&>(*op);
      if (aggregate.run_aggregate && aggregate.run_aggregate->IsGrouped() && op->sink_state) {
        aggregate.run_aggregate->ResetGrouped(*op->sink_state);
      }
      if (detached && op->sink_state) {
        detached->push_back(std::move(op->sink_state));
      }
    } else if (op->type == duckdb::PhysicalOperatorType::UNGROUPED_AGGREGATE) {
      auto& aggregate = static_cast<duckdb::PhysicalUngroupedAggregate&>(*op);
      if (aggregate.run_aggregate && !aggregate.run_aggregate->IsGrouped()) {
        aggregate.run_aggregate->Reset(duckdb::Allocator::DefaultAllocator());
      }
    }
    op->sink_state.reset();
    op->op_state.reset();
  }
}

// The lease return. Under the mutex, after an acquire fence, the plan's owner count:
// 1 - no context can still execute it: its state is dropped (still leased, so no
// statement takes it meanwhile) and the flag cleared; more - a context still owns it:
// the entry is evicted without the state drop. With `db` (a statement's return, async
// teardown enabled), the dropped aggregate states go to the reaper before the flag clears.
void ReturnLease(const std::shared_ptr<PlanEntry>& entry, uint64_t stmt,
                 duckdb::shared_ptr<duckdb::DatabaseInstance> db = nullptr) {
  auto& cache = GetPlanCache();
  long owners = 0;
  bool drop = false;
  {
    std::lock_guard<std::mutex> lock(cache.mutex);
    std::atomic_thread_fence(std::memory_order_acquire);
    owners = entry->data.use_count();
    if (entry->linked && owners == 1) {
      drop = true;
    } else {
      UnlinkEntry(cache.instances[entry->db], *entry);
      entry->leased = false;
    }
  }
  if (drop) {
    bool dropped = true;
    DetachedStates detached;
    try {
      DropPlanState(*entry->data, db ? &detached : nullptr);
    } catch (...) {
      dropped = false;
    }
    if (!detached.empty() &&
        !(dropped && TeardownReaper::Instance().Submit(detached, entry, db, stmt))) {
      detached.clear();  // inline, as with kAsyncTeardown false
    }
    std::lock_guard<std::mutex> lock(cache.mutex);
    if (!dropped) UnlinkEntry(cache.instances[entry->db], *entry);
    entry->leased = false;
  }
}

// A lease Create took, returned if Create exits before the statement holds it.
struct PlanLease {
  std::shared_ptr<PlanEntry> entry;
  uint64_t stmt = 0;
  duckdb::shared_ptr<duckdb::DatabaseInstance> db;  // set by a statement's return (async teardown)
  PlanLease() = default;
  PlanLease(const PlanLease&) = delete;
  PlanLease& operator=(const PlanLease&) = delete;
  ~PlanLease() {
    if (entry) ReturnLease(entry, stmt, std::move(db));
  }
};

// The session flag: set on every exit from Create unless the statement passed the
// statement test.
class SessionFlagGuard {
 public:
  explicit SessionFlagGuard(const std::string& session_id)
      : enabled_(PlanCacheEnabled()), session_id_(session_id) {}
  SessionFlagGuard(const SessionFlagGuard&) = delete;
  SessionFlagGuard& operator=(const SessionFlagGuard&) = delete;
  ~SessionFlagGuard() {
    if (enabled_ && !passed_) PlanCacheFlagSession(session_id_);
  }
  void Pass() { passed_ = true; }

 private:
  bool enabled_;
  bool passed_ = false;
  std::string session_id_;
};

// A pre-bump at construction and its one post-bump at destruction.
class GenerationPair {
 public:
  explicit GenerationPair(bool paired = true) : paired_(paired) {
    if (paired_) PlanCachePairBegin();
  }
  GenerationPair(const GenerationPair&) = delete;
  GenerationPair& operator=(const GenerationPair&) = delete;
  ~GenerationPair() {
    if (paired_) PlanCachePairEnd();
  }

 private:
  bool paired_;
};

template <class F>
auto Paired(F&& f) {
  GenerationPair pair;
  return f();
}

// The verdict Create stores on a statement object, with the lease it hands on.
void RegisterStatement(const DuckDBStatement* statement, bool cacheable, PlanLease& lease) {
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  auto& record = cache.statements[statement];
  record.cacheable = cacheable;
  record.stmt = lease.stmt;
  record.lease = std::move(lease.entry);
}

bool StatementCacheable(const DuckDBStatement* statement) {
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  auto found = cache.statements.find(statement);
  return found != cache.statements.end() && found->second.cacheable;
}

// The statement's record erased; its lease, if any, handed to the caller.
void ForgetStatement(const DuckDBStatement* statement, PlanLease& lease) {
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  auto found = cache.statements.find(statement);
  if (found == cache.statements.end()) return;
  lease.stmt = found->second.stmt;
  lease.entry = std::move(found->second.lease);
  cache.statements.erase(found);
}

struct PlanOutcome {
  std::shared_ptr<duckdb::PreparedStatement> stmt;
  bool cacheable = false;
  bool passes_e0 = false;
  uint64_t stmt_number = 0;
};

// One Create with the cache enabled: the tests in order, the
// lookup (a consulting Create only), the hit or the plain Prepare, the allowlist on a miss and
// the insert.
PlanOutcome LookupOrPrepare(const std::shared_ptr<ClientSession>& session,
                            const std::string& effective_sql,
                            const std::string& flight_method, bool is_internal,
                            bool has_settings_binds, PlanLease& lease) {
  PlanOutcome out;
  auto& cache = GetPlanCache();
  auto& connection = session->connection->Get();
  auto& context = *connection.context;
  const duckdb::DatabaseInstance* db = context.db.get();
  const bool consults = !is_internal && (flight_method == "DoGetStatement" ||
                                         flight_method == "GetFlightInfoStatement");

  const bool text_denied = TextDenylisted(effective_sql);
  const bool in_transaction = connection.HasActiveTransaction();
  bool flagged = false;
  std::string snapshot;
  std::string primary;
  bool primary_known = false;
  uint64_t gen = 0;
  int64_t inflight = 0;
  {
    std::lock_guard<std::mutex> lock(cache.mutex);
    auto& plans = cache.instances[db];
    primary = plans.primary;
    primary_known = plans.primary_known;
    if (auto it = cache.sessions.find(session->session_id); it != cache.sessions.end()) {
      flagged = it->second.flagged;
      if (it->second.snapshot_read) snapshot = it->second.snapshot;
    }
    gen = cache.generation;
    inflight = cache.inflight;
  }

  // the lookup
  const char* reason = nullptr;
  std::string key;
  std::shared_ptr<PlanEntry> hit;
  duckdb::shared_ptr<duckdb::PreparedStatementData> hit_data;
  std::string hit_query;
  if (consults && !text_denied && !in_transaction && !flagged && !has_settings_binds) {
    try {
      if (snapshot.empty()) {
        snapshot = ReadSettingsSnapshot(context);
        std::lock_guard<std::mutex> lock(cache.mutex);
        auto& record = cache.sessions[session->session_id];
        record.snapshot_read = true;
        record.snapshot = snapshot;
      }
      key = PlanKey(effective_sql, context, *session, snapshot);
      std::shared_ptr<PlanEntry> candidate;
      {
        std::lock_guard<std::mutex> lock(cache.mutex);
        gen = cache.generation;
        inflight = cache.inflight;
        auto& plans = cache.instances[db];
        if (inflight > 0) {
          reason = "miss-writer-inflight";
        } else if (auto found = plans.map.find(key); found == plans.map.end()) {
          reason = "miss-cold";
        } else if (auto entry = *found->second; entry->leased) {
          reason = "miss-leased";
        } else if (entry->generation != gen) {
          // never served; it stays until an insert of the key replaces it or the
          // capacity eviction takes it
          reason = "miss-generation";
        } else {
          entry->leased = true;
          plans.lru.splice(plans.lru.begin(), plans.lru, found->second);
          candidate = entry;
          hit_data = entry->data;
          hit_query = entry->query;
        }
      }
      if (candidate) {
        // the catalog-identity belt, inside a transaction (Create holds none)
        bool rebind = true;
        bool error = false;
        try {
          context.RunFunctionInTransaction(
              [&]() { rebind = hit_data->RequireRebind(context, nullptr); }, false);
        } catch (...) {
          error = true;
        }
        if (error || rebind) {
          std::lock_guard<std::mutex> lock(cache.mutex);
          UnlinkEntry(cache.instances[db], *candidate);
          candidate->leased = false;
          hit_data = nullptr;
          reason = error ? "miss-lookup-error" : "miss-rebind";
        } else {
          hit = candidate;
          reason = "hit";
        }
      }
    } catch (...) {
      reason = "miss-lookup-error";
    }
  }

  if (hit) {
    out.stmt = std::make_shared<duckdb::PreparedStatement>(
        connection.context, hit_data, hit_query, duckdb::case_insensitive_map_t<duckdb::idx_t>());
    lease.entry = hit;
  } else {
    out.stmt = connection.Prepare(effective_sql);
  }

  // the tests in order; the first failure names the reason
  const char* ineligible = nullptr;
  bool prepared = true;
  if (hit) {
    out.passes_e0 = true;
  } else {
    const char* e0 = StatementTest(*out.stmt, in_transaction, primary, primary_known);
    out.passes_e0 = e0 == nullptr;
    prepared = !(e0 != nullptr && std::string_view(e0) == "single");
    if (text_denied) {
      ineligible = "text";
    } else if (e0 != nullptr && prepared) {
      ineligible = e0;
    } else if (!prepared && in_transaction) {
      ineligible = "transaction";
    } else if (flagged) {
      ineligible = "session";
    } else if (prepared && (!out.stmt->data->properties.bound_all_parameters ||
                            out.stmt->data->properties.parameter_count != 0 ||
                            !out.stmt->data->value_map.empty())) {
      ineligible = "parameters";
    } else if (prepared && out.stmt->data->properties.always_require_rebind) {
      ineligible = "rebind";
    } else if (has_settings_binds) {
      ineligible = "settings";
    }
  }

  // EA on a miss that could be cacheable, then the insert
  if (hit) {
    out.cacheable = true;
  } else if (consults && prepared && ineligible == nullptr && reason != nullptr &&
             !key.empty() && std::string_view(reason) != "miss-writer-inflight") {
    ineligible = AllowlistTest(*out.stmt->data, primary);
    out.cacheable = ineligible == nullptr;
  }
  {
    std::lock_guard<std::mutex> lock(cache.mutex);
    auto& plans = cache.instances[db];
    if (out.cacheable && !hit && std::string_view(reason) != "miss-leased") {
      // no insert while a writer is in flight or once the generation moved
      if (cache.inflight <= 0 && cache.generation == gen) {
        if (auto found = plans.map.find(key); found != plans.map.end()) {
          UnlinkEntry(plans, **found->second);
        }
        bool room = plans.map.size() < kPlanCacheEntries;
        for (auto it = plans.lru.end(); !room && it != plans.lru.begin();) {
          --it;
          if (!(*it)->leased) {
            UnlinkEntry(plans, **it);
            room = true;
          }
        }
        if (room) {
          auto entry = std::make_shared<PlanEntry>();
          entry->db = db;
          entry->key = key;
          entry->data = out.stmt->data;
          entry->query = out.stmt->query;
          entry->username = session->username;
          entry->role = session->role;
          entry->generation = gen;
          entry->leased = true;
          entry->linked = true;
          plans.lru.push_front(entry);
          plans.map[key] = plans.lru.begin();
          lease.entry = entry;
        }
      }
    }
  }

  if (consults) {
    out.stmt_number = ++cache.next_stmt;
    lease.stmt = out.stmt_number;
  }
  return out;
}

}  // namespace

bool PlanCacheEnabled() {
  return gizmosql::kPlanCache;
}

void PlanCachePairBegin() {
  if (!PlanCacheEnabled()) return;
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  ++cache.generation;
  ++cache.inflight;
}

void PlanCachePairEnd() {
  if (!PlanCacheEnabled()) return;
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  ++cache.generation;
  --cache.inflight;
}

void PlanCacheSetPrimaryCatalog(const duckdb::DatabaseInstance* db, const std::string& name) {
  if (!PlanCacheEnabled()) return;
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  auto& plans = cache.instances[db];
  plans.primary = name;
  plans.primary_known = true;
}

void PlanCacheFlagSession(const std::string& session_id) {
  if (!PlanCacheEnabled()) return;
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  cache.sessions[session_id].flagged = true;
}

void PlanCacheForgetSession(const std::string& session_id) {
  if (!PlanCacheEnabled()) return;
  auto& cache = GetPlanCache();
  std::lock_guard<std::mutex> lock(cache.mutex);
  cache.sessions.erase(session_id);
}

// Async teardown: returns once no reaper job waits or runs (a no-op with kAsyncTeardown false).
void DrainTeardownReaper() {
  if (!AsyncTeardownEnabled()) return;
  TeardownReaper::Instance().Drain();
}

// The server's own statements (a fresh connection per call): a text passing the statement test -
// the health check's SELECT 1 - never pairs; any other pairs. Classified once per
// text by a Prepare on such a connection.
bool PlanCacheServerTextPairs(duckdb::Connection& connection, const std::string& sql) {
  if (!PlanCacheEnabled()) return false;
  auto& cache = GetPlanCache();
  const duckdb::DatabaseInstance* db = connection.context->db.get();
  std::string primary;
  bool primary_known = false;
  {
    std::lock_guard<std::mutex> lock(cache.mutex);
    if (auto found = cache.server_texts.find(sql); found != cache.server_texts.end()) {
      return found->second;
    }
    auto& plans = cache.instances[db];
    primary = plans.primary;
    primary_known = plans.primary_known;
  }
  bool pairs = true;
  try {
    const bool in_transaction = connection.HasActiveTransaction();
    auto stmt = connection.Prepare(sql);
    pairs = !stmt || StatementTest(*stmt, in_transaction, primary, primary_known) != nullptr;
  } catch (...) {
    pairs = true;
  }
  std::lock_guard<std::mutex> lock(cache.mutex);
  cache.server_texts[sql] = pairs;
  return pairs;
}

namespace {
// Prepare once: the statement counts of the sessions that have asked for one (a
// GetFlightInfoStatement with prepare once on); never destroyed, as it may be used until
// exit.
struct SessionStatementCounts {
  std::mutex mutex;
  std::unordered_map<std::string, uint64_t> counts;
};

SessionStatementCounts& GetSessionStatementCounts() {
  static auto* counts = new SessionStatementCounts();
  return *counts;
}
}  // namespace

uint64_t SessionStatementCount(const std::string& session_id) {
  auto& s = GetSessionStatementCounts();
  std::lock_guard<std::mutex> lock(s.mutex);
  return s.counts[session_id];
}

void CountSessionStatement(const std::string& session_id) {
  auto& s = GetSessionStatementCounts();
  std::lock_guard<std::mutex> lock(s.mutex);
  if (auto it = s.counts.find(session_id); it != s.counts.end()) {
    ++it->second;
  }
}

void ForgetSessionStatementCount(const std::string& session_id) {
  auto& s = GetSessionStatementCounts();
  std::lock_guard<std::mutex> lock(s.mutex);
  s.counts.erase(session_id);
}

namespace {
// Defined with the GizmoSQL settings registry below; forward-declared here because
// DuckDBStatement::Create() uses it for the gizmosql_settings() table function.
std::string RewriteGizmoSettings(const std::string& sql, const ClientSession& session,
                                 DuckDBFlightSqlServer* server,
                                 duckdb::vector<duckdb::Value>& binds);
}  // namespace
std::shared_ptr<arrow::DataType> GetDataTypeFromDuckDbType(
    const duckdb::LogicalType& duckdb_type) {
  switch (duckdb_type.id()) {
    case duckdb::LogicalTypeId::INTEGER:
      return arrow::int32();
    case duckdb::LogicalTypeId::DECIMAL: {
      uint8_t width = duckdb::DecimalType::GetWidth(duckdb_type);
      uint8_t scale = duckdb::DecimalType::GetScale(duckdb_type);
      // Always emit Decimal128 (or Decimal256 when the precision exceeds 38).
      // DuckDB reports width=0 for parameters whose precision wasn't resolved
      // during parse (e.g. `?` in `INSERT INTO t(dec) VALUES (?)`) — use the
      // widest decimal in that case. Also, the Arrow Java JDBC client only
      // supports Decimal128/Decimal256, so we must not hand it a Decimal32/64
      // (which is what arrow::smallest_decimal would pick for width<=18).
      if (width == 0 || width > 38) {
        return arrow::decimal256(width == 0 ? 38 : width, scale);
      }
      return arrow::decimal128(width, scale);
    }
    case duckdb::LogicalTypeId::FLOAT:
      return arrow::float32();
    case duckdb::LogicalTypeId::DOUBLE:
      return arrow::float64();
    case duckdb::LogicalTypeId::CHAR:
    case duckdb::LogicalTypeId::VARCHAR:
      return arrow::utf8();
    case duckdb::LogicalTypeId::BLOB:
      return arrow::binary();
    case duckdb::LogicalTypeId::TINYINT:
      return arrow::int8();
    case duckdb::LogicalTypeId::SMALLINT:
      return arrow::int16();
    case duckdb::LogicalTypeId::BIGINT:
      return arrow::int64();
    case duckdb::LogicalTypeId::BOOLEAN:
      return arrow::boolean();
    case duckdb::LogicalTypeId::DATE:
      return arrow::date32();
    case duckdb::LogicalTypeId::TIME:
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
      return timestamp(arrow::TimeUnit::MILLI);
    case duckdb::LogicalTypeId::TIMESTAMP:
      return timestamp(arrow::TimeUnit::MICRO);
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      return timestamp(arrow::TimeUnit::SECOND);
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
      return timestamp(arrow::TimeUnit::NANO);
    case duckdb::LogicalTypeId::INTERVAL:
      return duration(
          arrow::TimeUnit::MICRO);  // ASSUMING MICRO AS DUCKDB's DOCS DOES NOT SPECIFY
    case duckdb::LogicalTypeId::UTINYINT:
      return arrow::uint8();
    case duckdb::LogicalTypeId::USMALLINT:
      return arrow::uint16();
    case duckdb::LogicalTypeId::UINTEGER:
      return arrow::uint32();
    case duckdb::LogicalTypeId::UBIGINT:
      return arrow::int64();
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    case duckdb::LogicalTypeId::TIME_TZ:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case duckdb::LogicalTypeId::HUGEINT:
      return arrow::decimal128(38, 0);
    case duckdb::LogicalTypeId::INVALID:
    case duckdb::LogicalTypeId::SQLNULL:
    case duckdb::LogicalTypeId::UNKNOWN:
    case duckdb::LogicalTypeId::ANY:
#if GIZMOSQL_DUCKDB_CHANNEL_LTS
    // DuckDB v1.4.x still carries LogicalTypeId::USER as a distinct enum
    // value; v1.5+ folded it into UNBOUND, so the case is omitted there.
    case duckdb::LogicalTypeId::USER:
#endif
      return arrow::null();
    case duckdb::LogicalTypeId::LIST: {
      auto child_type = duckdb::ListType::GetChildType(duckdb_type);
      return arrow::list(GetDataTypeFromDuckDbType(child_type));
    }
    case duckdb::LogicalTypeId::STRUCT: {
      auto& child_types = duckdb::StructType::GetChildTypes(duckdb_type);
      arrow::FieldVector fields;
      for (auto& child : child_types) {
        fields.push_back(
            arrow::field(child.first, GetDataTypeFromDuckDbType(child.second)));
      }
      return arrow::struct_(fields);
    }
    case duckdb::LogicalTypeId::MAP: {
      auto key_type = duckdb::MapType::KeyType(duckdb_type);
      auto value_type = duckdb::MapType::ValueType(duckdb_type);
      return arrow::map(GetDataTypeFromDuckDbType(key_type),
                        GetDataTypeFromDuckDbType(value_type));
    }
    case duckdb::LogicalTypeId::ARRAY: {
      auto child_type = duckdb::ArrayType::GetChildType(duckdb_type);
      auto array_size = duckdb::ArrayType::GetSize(duckdb_type);
      return arrow::fixed_size_list(GetDataTypeFromDuckDbType(child_type), array_size);
    }
#if !GIZMOSQL_DUCKDB_CHANNEL_LTS
    case duckdb::LogicalTypeId::VARIANT:
      // VARIANT is self-describing typed binary data (DuckDB v1.5.0+).
      // DuckDB's Arrow exporter does not yet support VARIANT natively, so
      // clients should cast to VARCHAR or JSON before querying:
      //   SELECT v::VARCHAR FROM t;
      // Not present in the LTS channel (v1.4.x).
      return arrow::binary();
#endif
    case duckdb::LogicalTypeId::POINTER:
    case duckdb::LogicalTypeId::VALIDITY:
    case duckdb::LogicalTypeId::UUID:
    case duckdb::LogicalTypeId::TABLE:
    case duckdb::LogicalTypeId::ENUM:
    default:
      return arrow::null();
  }
}

arrow::Result<arrow::util::ArrowLogLevel> GetSessionOrServerLogLevel(
    const std::shared_ptr<ClientSession>& client_session) {
  // Fall-back to the session query log level if the statement log level is not set...
  if (client_session->query_log_level.has_value()) {
    return client_session->query_log_level.value();
  }
  // Fall-back to the server's setting if the session setting is not set...
  if (auto server = GetServer(*client_session)) {
    return server->GetQueryLogLevel(*client_session);
  }
  return arrow::Status::Invalid("Unable to get server instance");
}

// Resolve the effective query-profile-capture mode for a session: the per-session
// override wins, otherwise fall back to the server default. Returns kOff if the
// server instance is gone (capture is a best-effort, never-fail feature).
QueryProfileMode GetSessionOrServerCaptureProfile(
    const std::shared_ptr<ClientSession>& client_session) {
  if (client_session->capture_query_profile.has_value()) {
    return client_session->capture_query_profile.value();
  }
  if (auto server = GetServer(*client_session)) {
    return server->GetCaptureQueryProfile(*client_session);
  }
  return QueryProfileMode::kOff;
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    const std::shared_ptr<ClientSession>& client_session, const std::string& handle,
    const std::string& sql, const std::optional<arrow::util::ArrowLogLevel>& log_level,
    const bool& log_queries, const std::shared_ptr<arrow::Schema>& override_schema,
    const std::string& flight_method, bool is_internal) {
  // the plan cache's session flag, set on every path unless the statement passes the statement test
  SessionFlagGuard plan_flag_guard(client_session->session_id);
  std::string status;
  auto logged_sql = redact_sql_for_logs(sql);
  // Threshold: session/server log level gates whether messages are emitted
  ARROW_ASSIGN_OR_RAISE(auto log_threshold,
                        GetSessionOrServerLogLevel(client_session));
  // Display severity: statement's own level, defaulting to INFO for user queries
  auto display_severity =
      log_level.value_or(arrow::util::ArrowLogLevel::ARROW_INFO);

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Create", status, {"peer", client_session->peer},
      {"session_id", client_session->session_id}, {"user", client_session->username},
      {"role", client_session->role}, {"statement_id", handle});

  client_session->active_sql_handle = handle;

  if (!is_internal) {
    client_session->TouchSqlActivity();
  }

  // Rudimentary admin-command gate (Core): block dangerous filesystem- and
  // instance-level commands (ATTACH/DETACH, SET GLOBAL, INSTALL/LOAD, CHECKPOINT,
  // COPY/EXPORT to local files, read_* of local files, duckdb_secrets()) for
  // non-admin sessions, ahead of the full RBAC model (GizmoSQL 2.0). Basic-auth
  // users are always "admin", so this only affects token deployments with
  // non-admin roles. Internal/system queries are exempt. Admins and internal
  // queries skip the parse entirely (fast path).
  if (!is_internal && client_session->role != "admin") {
    if (auto gated_category = gizmosql::ddb::ClassifyGatedCommand(sql)) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Client attempted a gated admin command",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"gated", *gated_category}, {"statement_id", handle},
                             {"sql", logged_sql});
#ifdef GIZMOSQL_ENTERPRISE
      std::string gate_error_msg =
          "Permission denied: " + *gated_category +
          " requires the 'admin' role. This GizmoSQL instance restricts "
          "filesystem- and instance-level commands to admin users.";
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, gate_error_msg);
        }
      }
#endif
      return gizmosql::ddb::CheckNonAdminCommandAllowed(sql);
    }
  }

  // Get instance_id + cluster_id from the server for the
  // GIZMOSQL_CURRENT_INSTANCE() / GIZMOSQL_CURRENT_CLUSTER() pseudo-functions.
  std::string instance_id;
  std::string cluster_id;
  if (auto server = GetServer(*client_session)) {
    instance_id = server->GetInstanceId();
    cluster_id = server->GetClusterId();
  }

  // Get edition name for GIZMOSQL_EDITION()
#ifdef GIZMOSQL_ENTERPRISE
  std::string edition = enterprise::EnterpriseFeatures::Instance().GetEditionName();
#else
  std::string edition = "Core";
#endif

  // Get instrumentation state for GIZMOSQL_INSTRUMENTATION_*() functions
  bool instrumentation_enabled = false;
  std::string instrumentation_catalog;
  std::string instrumentation_schema;
#ifdef GIZMOSQL_ENTERPRISE
  if (auto server = GetServer(*client_session)) {
    if (auto mgr = server->GetInstrumentationManager()) {
      instrumentation_enabled = true;
      instrumentation_catalog = mgr->GetCatalog();
      instrumentation_schema = mgr->GetSchema();
    }
  }
#endif

  // Replace GIZMOSQL_* pseudo-functions with actual values
  std::string effective_sql = ReplaceGizmoSQLFunctions(
      sql, client_session->session_id, instance_id, cluster_id, client_session->username,
      client_session->role, edition, instrumentation_enabled,
      instrumentation_catalog, instrumentation_schema);

  // gizmosql_settings() table function: rewrite to a bind-parameterized VALUES so
  // its rows (incl. JSON tags / descriptions) are passed as bound parameters. The
  // collected binds are attached to the prepared statement below.
  duckdb::vector<duckdb::Value> settings_binds;
  if (boost::icontains(effective_sql, "gizmosql_settings()")) {
    auto settings_server = GetServer(*client_session);
    effective_sql = RewriteGizmoSettings(effective_sql, *client_session,
                                         settings_server.get(), settings_binds);
  }

#ifdef GIZMOSQL_ENTERPRISE
  std::shared_ptr<InstrumentationManager> instr_mgr;
  std::string log_catalog;
  if (auto server = GetServer(*client_session)) {
    instr_mgr = server->GetInstrumentationManager();
    log_catalog = server->GetLogCatalog();
  }

  if (!is_internal) {
    if (auto use_catalog_name = TryExtractUseCatalogName(sql)) {
      if (Paired([&] {
            return CatalogExistsOnConnection(client_session->connection->Get(),
                                             *use_catalog_name);
          })) {
        ARROW_RETURN_NOT_OK(gizmosql::enterprise::EnsureCatalogReadAccess(
            client_session, *use_catalog_name, instr_mgr, handle, logged_sql,
            flight_method, is_internal, log_catalog));
      }
    }
  }

  // Catalog visibility filtering: rewrite metadata queries to hide unauthorized catalogs
  if (!client_session->catalog_access.empty() &&
      enterprise::EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable()) {
    auto allowed = Paired([&] {
      return enterprise::GetAllowedCatalogs(*client_session,
                                            client_session->connection->Get(), instr_mgr,
                                            log_catalog);
    });
    if (!allowed.empty()) {
      auto filter_in = enterprise::BuildCatalogFilterIN(allowed);
      std::string rewritten;
      if (enterprise::RewriteShowCommand(effective_sql, filter_in, rewritten)) {
        GIZMOSQL_LOGKV_SESSION(DEBUG, client_session,
                               "Catalog visibility filter rewrote SHOW command",
                               {"kind", "sql"}, {"original_sql", effective_sql},
                               {"rewritten_sql", rewritten});
        effective_sql = std::move(rewritten);
      } else {
        auto filtered = enterprise::FilterMetadataReferences(effective_sql, filter_in);
        if (filtered != effective_sql) {
          GIZMOSQL_LOGKV_SESSION(DEBUG, client_session,
                                 "Catalog visibility filter rewrote metadata references",
                                 {"kind", "sql"}, {"original_sql", effective_sql},
                                 {"rewritten_sql", filtered});
          effective_sql = std::move(filtered);
        }
      }
    }
  }
#endif

  if (log_queries) {
    GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
        log_threshold, display_severity,
        client_session, "Client is attempting to run a SQL command",
        {"kind", "sql"}, {"status", "attempt"}, {"statement_id", handle},
        {"sql", logged_sql}, {"is_internal", is_internal ? "true" : "false"},
        {"flight_method", flight_method});
  }

  // Prevent DETACH of the system-managed catalogs — instrumentation and
  // catalog-logging — including externally-attached ones (e.g. DuckLake/PostgreSQL).
#ifdef GIZMOSQL_ENTERPRISE
  {
    std::string instr_catalog;
    std::string log_catalog;
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        instr_catalog = mgr->GetCatalog();
      }
      log_catalog = server->GetLogCatalog();
    }
    const char* detach_target = nullptr;
    if (IsDetachInstrumentationDb(sql, instr_catalog)) {
      detach_target = "instrumentation";
    } else if (SqlDetachesCatalog(sql, log_catalog)) {
      detach_target = "catalog-logging";
    }
    if (detach_target != nullptr) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                     "Client attempted to DETACH a system-managed database",
                     {"kind", "sql"}, {"status", "rejected"}, {"catalog", detach_target},
                     {"statement_id", handle}, {"sql", logged_sql});
      std::string error_msg =
          std::string("Cannot DETACH the ") + detach_target + " database";
      // Record the rejected DETACH attempt
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, error_msg);
        }
      }
      return Status::Invalid(error_msg);
    }
  }
#endif

  // Handle KILL SESSION command
  std::string target_session_id;
#ifdef GIZMOSQL_ENTERPRISE
  if (gizmosql::enterprise::IsKillSessionCommand(sql, target_session_id)) {
    auto server = GetServer(*client_session);
    std::shared_ptr<InstrumentationManager> instr_mgr;
    if (server) {
      instr_mgr = server->GetInstrumentationManager();
    }

    auto kill_status = gizmosql::enterprise::HandleKillSession(
        client_session, target_session_id, server.get(), instr_mgr,
        handle, logged_sql, flight_method, is_internal);

    if (!kill_status.ok()) {
      return kill_status;
    }

    // Return a synthetic result for the successful KILL SESSION command
    std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
        client_session, handle, sql, display_severity, log_queries, override_schema,
        is_internal, flight_method));
    result->is_gizmosql_admin_ = true;

    // Create statement instrumentation for successful KILL SESSION
    if (instr_mgr) {
      result->instrumentation_ = std::make_unique<StatementInstrumentation>(
          instr_mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
          "", client_session->query_tag);
    }

    // Create a synthetic result batch with success message
    auto schema = arrow::schema({arrow::field("result", arrow::utf8())});
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Append("Session " + target_session_id + " killed successfully"));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    result->synthetic_result_batch_ = arrow::RecordBatch::Make(
        schema, 1, std::vector<std::shared_ptr<arrow::Array>>{array});

    return result;
  }
#else
  // Core edition: KILL SESSION requires enterprise license
  if (IsKillSessionCommand(sql, target_session_id)) {
    std::string error_msg = "KILL SESSION is a commercially licensed enterprise feature. "
                            "Please provide a valid license key file via --license-key-file "
                            "or contact GizmoData sales at sales@gizmodata.com to obtain a license.";
    GIZMOSQL_LOGKV_SESSION(WARNING, client_session, "KILL SESSION attempted without enterprise license",
                   {"kind", "sql"}, {"status", "rejected"},
                   {"target_session_id", target_session_id});
    return Status::Invalid(error_msg);
  }
#endif

  if (IsLikelyGizmoSQLSet(sql)) {
    std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
        client_session, handle, sql, display_severity, log_queries, override_schema,
        is_internal, flight_method));
    result->is_gizmosql_admin_ = true;

#ifdef GIZMOSQL_ENTERPRISE
    // Create statement instrumentation (admin commands are still tracked)
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        result->instrumentation_ = std::make_unique<StatementInstrumentation>(
            mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
            "", client_session->query_tag);
      }
    }
#endif

    if (log_queries) {
      GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
          log_threshold, display_severity,
          client_session, "Detected GizmoSQL admin SET command",
          {"kind", "sql"}, {"status", "admin"}, {"statement_id", handle},
          {"sql", logged_sql}, {"is_internal", is_internal ? "true" : "false"},
          {"flight_method", flight_method});
    }
    return result;
  }

  // The plan cache's lookup-or-Prepare; with the cache off, the plain Prepare. A lease taken here returns if Create exits before the
  // statement holds it (declared before `stmt`, so the plan's holder is released first).
  PlanLease plan_lease;
  bool plan_cacheable = false;
  std::shared_ptr<duckdb::PreparedStatement> stmt;
  if (PlanCacheEnabled()) {
    auto outcome = LookupOrPrepare(client_session, effective_sql, flight_method, is_internal,
                                   !settings_binds.empty(), plan_lease);
    stmt = std::move(outcome.stmt);
    plan_cacheable = outcome.cacheable;
    if (outcome.passes_e0) plan_flag_guard.Pass();
  } else {
    stmt = client_session->connection->Get().Prepare(effective_sql);
  }

  if (stmt->success) {
    // Block writes to the GizmoSQL system catalog regardless of role or
    // licensing — it is a process-local in-memory catalog that hosts
    // server-managed metadata views, and clients must not be able to
    // mutate them. Enforced in both Core and Enterprise builds. The
    // analogous protection for the instrumentation catalog
    // (_gizmosql_instr) lives in CheckCatalogWriteAccess and only runs
    // in Enterprise builds because that catalog only exists there.
    for (const auto& [catalog_name, _ignored] :
         stmt->data->properties.modified_databases) {
      if (gizmosql::IsSystemCatalog(catalog_name)) {
        std::string error_msg =
            "Access denied: The GizmoSQL system catalog '" + catalog_name +
            "' is read-only.";
        GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                               "Access denied: system catalog is read-only",
                               {"kind", "sql"}, {"status", "rejected"},
                               {"catalog", catalog_name},
                               {"statement_id", handle},
                               {"sql", logged_sql});
#ifdef GIZMOSQL_ENTERPRISE
        if (auto server = GetServer(*client_session)) {
          if (auto mgr = server->GetInstrumentationManager()) {
            StatementInstrumentation(mgr, handle, client_session->session_id,
                                     logged_sql, flight_method, is_internal,
                                     error_msg);
          }
        }
#endif
        return arrow::Status::Invalid(error_msg);
      }
    }

#ifdef GIZMOSQL_ENTERPRISE
    // Check catalog-level access permissions (Enterprise feature)
    // These checks enforce per-catalog read/write permissions from JWT token claims
    std::shared_ptr<InstrumentationManager> instr_mgr;
    std::string log_catalog;
    if (auto server = GetServer(*client_session)) {
      instr_mgr = server->GetInstrumentationManager();
      log_catalog = server->GetLogCatalog();
    }

    // Check write access for all catalogs the statement will modify
    auto write_status = gizmosql::enterprise::CheckCatalogWriteAccess(
        client_session, stmt->data->properties.modified_databases,
        instr_mgr, handle, logged_sql, flight_method, is_internal, log_catalog);
    if (!write_status.ok()) {
      return write_status;
    }

    // Check read access for all catalogs the statement will read
    auto read_status = gizmosql::enterprise::CheckCatalogReadAccess(
        client_session, stmt->data->properties.read_databases,
        instr_mgr, handle, logged_sql, flight_method, is_internal, log_catalog);
    if (!read_status.ok()) {
      return read_status;
    }
#endif

    // Check for readonly role trying to modify data (legacy support)
    if (!stmt->data->properties.IsReadOnly() && client_session->role == "readonly") {
      std::string error_msg =
          "User '" + client_session->username +
          "' has a readonly session and cannot run statements that modify state.";
#ifdef GIZMOSQL_ENTERPRISE
      // Record the rejected modification attempt by readonly user
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, error_msg);
        }
      }
#endif
      return Status::ExecutionError(error_msg);
    }
  }

  if (not stmt->success) {
    std::string error_message = stmt->error.Message();

    // Check if this is the multiple statements error that can be resolved with direct execution
    if (error_message.find("Cannot prepare multiple statements at once") !=
        std::string::npos) {
      // Fallback to direct query execution for statements like PIVOT that get rewritten to multiple statements
      if (log_queries) {
        GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
            log_threshold, display_severity,
            client_session,
            "SQL command cannot run as a prepared statement, falling back to direct "
            "query execution",
            {"kind", "sql"}, {"status", "fallback"},
            {"statement_id", handle}, {"sql", logged_sql},
            {"is_internal", is_internal ? "true" : "false"},
            {"flight_method", flight_method});
      }

      std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
          client_session, handle, effective_sql, log_level, log_queries, override_schema,
          is_internal, flight_method));

#ifdef GIZMOSQL_ENTERPRISE
      // Create statement instrumentation for direct execution
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          result->instrumentation_ = std::make_unique<StatementInstrumentation>(
              mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
              "", client_session->query_tag);
        }
      }
#endif

      status = "success";
      return result;
    }

    // Other preparation errors are still fatal
    std::string err_msg =
        "Can't prepare statement: '" + logged_sql + "' - Error: " + error_message;

    if (log_queries) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session, "Client SQL command failed preparation",
                     {"kind", "sql"}, {"status", "failure"},
                     {"statement_id", handle}, {"error", err_msg},
                     {"sql", logged_sql});
    }

#ifdef GIZMOSQL_ENTERPRISE
    // Record the failed statement in instrumentation with the error message
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        // Create instrumentation record for the failed statement (fire and forget)
        StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                 flight_method, is_internal, error_message);
      }
    }
#endif

    return Status::Invalid(err_msg);
  }

  std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
      client_session, handle, stmt, log_level, log_queries, override_schema, is_internal,
      flight_method));
  if (PlanCacheEnabled()) {
    RegisterStatement(result.get(), plan_cacheable, plan_lease);
  }

  // Bind the gizmosql_settings() values (if this statement referenced it).
  if (!settings_binds.empty()) {
    result->bind_parameters = std::move(settings_binds);
  }

#ifdef GIZMOSQL_ENTERPRISE
  // Create statement instrumentation for prepared statement
  if (auto server = GetServer(*client_session)) {
    if (auto mgr = server->GetInstrumentationManager()) {
      result->instrumentation_ = std::make_unique<StatementInstrumentation>(
          mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
          "", client_session->query_tag);
    }
  }
#endif

  status = "success";
  return result;
}

arrow::Status DuckDBStatement::ReuseForDoGet() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());
  ARROW_ASSIGN_OR_RAISE(auto log_threshold, GetSessionOrServerLogLevel(session));
  session->active_sql_handle = statement_id_;
  if (!is_internal_) {
    session->TouchSqlActivity();
  }
  // DoGetStatement creates its statement with no display severity of its own (INFO)
  log_level_ = std::nullopt;
  if (log_queries_) {
    GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
        log_threshold, arrow::util::ArrowLogLevel::ARROW_INFO,
        session, "Client is attempting to run a SQL command",
        {"kind", "sql"}, {"status", "reused"}, {"statement_id", statement_id_},
        {"sql", logged_sql_}, {"is_internal", is_internal_ ? "true" : "false"},
        {"flight_method", "DoGetStatement"});
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    const std::shared_ptr<ClientSession>& client_session, const std::string& sql,
    const std::optional<arrow::util::ArrowLogLevel>& log_level, const bool& log_queries,
    const std::shared_ptr<arrow::Schema>& override_schema,
    const std::string& flight_method, bool is_internal) {
  std::string handle = NewUuid();
  return DuckDBStatement::Create(client_session, handle, sql, log_level, log_queries,
                                 override_schema, flight_method, is_internal);
}

namespace {

// ---- GizmoSQL settings registry --------------------------------------------
// Single source of truth for the gizmosql.* session/server settings. Each
// descriptor carries metadata plus the mutator hooks invoked by
// SettingsRegistry::Apply(), which centralizes the cross-cutting checks:
// unknown-name, enterprise license, scope validity, and the admin gate for
// GLOBAL writes. Adding a setting = one descriptor.
enum class SetScopeKind { kSessionOnly, kGlobalOnly, kSessionOrGlobal };

struct GizmoSetting {
  std::string name;
  SetScopeKind scope;
  bool enterprise = false;
  const char* enterprise_feature = nullptr;  // mirrors enterprise::kFeature* values
  std::string input_type;                    // INTEGER / BOOLEAN / VARCHAR (display)
  std::string env_var;                       // "" if none
  std::string default_value;
  std::string description;
  // Value accessors for gizmosql_settings() (canonical strings; nullopt = unset /
  // not-applicable-at-this-scope).
  std::function<std::optional<std::string>(const ClientSession&)> get_session;
  std::function<std::optional<std::string>(DuckDBFlightSqlServer&, const ClientSession&)>
      get_global;
  std::function<arrow::Status(ClientSession&, const std::string&)> set_session;
  std::function<arrow::Status(DuckDBFlightSqlServer&, ClientSession&,
                              const std::string&)>
      set_global;
};

// Parse a SET value into a bool, tolerating the forms DuckDB's parser produces:
// integer/string constants and the `true`/`false` cast-literal text.
arrow::Result<bool> ParseSetBool(const std::string& val, const std::string& setting) {
  const std::string lowered = boost::algorithm::to_lower_copy(val);
  if (lowered == "true" || lowered == "1" || lowered == "on" || lowered == "yes" ||
      lowered == "t" || lowered == "'true'" || lowered == "cast('t' as boolean)") {
    return true;
  }
  if (lowered == "false" || lowered == "0" || lowered == "off" || lowered == "no" ||
      lowered == "f" || lowered == "'false'" || lowered == "cast('f' as boolean)") {
    return false;
  }
  return arrow::Status::Invalid("Invalid value for " + setting + ": " + val);
}

arrow::Result<int32_t> ParseSetInt(const std::string& val, const std::string& setting) {
  try {
    return static_cast<int32_t>(std::stoi(val));
  } catch (...) {
    return arrow::Status::Invalid("Invalid value for " + setting + ": " + val);
  }
}

class SettingsRegistry {
 public:
  static const SettingsRegistry& Instance() {
    static const SettingsRegistry kRegistry;
    return kRegistry;
  }

  const GizmoSetting* Find(const std::string& name) const {
    auto it = by_name_.find(name);
    return it == by_name_.end() ? nullptr : &settings_[it->second];
  }

  const std::vector<GizmoSetting>& All() const { return settings_; }

  // Dispatch `SET [SESSION|GLOBAL] <name> = <val>` after centralized checks.
  arrow::Status Apply(ClientSession& session, DuckDBFlightSqlServer* server,
                      const std::string& name, duckdb::SetScope scope,
                      const std::string& val) const {
    const GizmoSetting* d = Find(name);
    if (!d) {
      return arrow::Status::Invalid("Unknown GizmoSQL configuration parameter: " + name);
    }

    // Enterprise license gate (centralized).
    if (d->enterprise) {
#ifdef GIZMOSQL_ENTERPRISE
      if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsFeatureAvailable(
              d->enterprise_feature)) {
        return arrow::Status::Invalid(
            gizmosql::enterprise::EnterpriseFeatures::GetLicenseRequiredError("SET " +
                                                                              d->name));
      }
#else
      return arrow::Status::Invalid(
          "SET " + d->name +
          " is a commercially licensed enterprise feature. Please provide a valid "
          "license key file via --license-key-file or contact GizmoData sales at "
          "sales@gizmodata.com to obtain a license.");
#endif
    }

    // Resolve effective scope. A bare SET (AUTOMATIC) on a global-only setting
    // means GLOBAL; otherwise a non-GLOBAL scope means SESSION.
    const bool to_global = scope == duckdb::SetScope::GLOBAL ||
                           (scope != duckdb::SetScope::GLOBAL &&
                            d->scope == SetScopeKind::kGlobalOnly);
    if (to_global && d->scope == SetScopeKind::kSessionOnly) {
      return arrow::Status::Invalid(d->name + " can only be set at SESSION scope");
    }
    if (!to_global && d->scope == SetScopeKind::kGlobalOnly) {
      return arrow::Status::Invalid(d->name + " can only be set with SET GLOBAL");
    }

    if (to_global) {
      if (session.role != "admin") {
        return arrow::Status::Invalid("Only admin users can SET GLOBAL " + d->name);
      }
      if (!server || !d->set_global) {
        return arrow::Status::Invalid(d->name + " cannot be set at GLOBAL scope");
      }
      return d->set_global(*server, session, val);
    }
    if (!d->set_session) {
      return arrow::Status::Invalid(d->name + " cannot be set at SESSION scope");
    }
    return d->set_session(session, val);
  }

 private:
  SettingsRegistry();
  std::vector<GizmoSetting> settings_;
  std::unordered_map<std::string, size_t> by_name_;
};

SettingsRegistry::SettingsRegistry() {
  settings_.push_back(GizmoSetting{
      .name = "gizmosql.query_timeout",
      .scope = SetScopeKind::kSessionOrGlobal,
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_QUERY_TIMEOUT",
      .default_value = "0",
      .description = "Per-statement timeout in seconds (0 = no timeout).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.query_timeout ? std::optional(std::to_string(*s.query_timeout))
                               : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession& s) -> std::optional<std::string> {
        auto r = srv.GetQueryTimeout(s);
        return r.ok() ? std::optional(std::to_string(*r)) : std::nullopt;
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "query_timeout"));
        s.query_timeout = n;
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession& s,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "query_timeout"));
        return srv.SetQueryTimeout(s, n);
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.query_log_level",
      .scope = SetScopeKind::kSessionOrGlobal,
      .input_type = "VARCHAR",
      .env_var = "GIZMOSQL_QUERY_LOG_LEVEL",
      .default_value = "INFO",
      .description = "Query-execution log level (DEBUG/INFO/WARNING/ERROR).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.query_log_level ? std::optional(log_level_arrow_log_level_to_string(
                                       *s.query_log_level))
                                 : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession& s) -> std::optional<std::string> {
        auto r = srv.GetQueryLogLevel(s);
        return r.ok() ? std::optional(log_level_arrow_log_level_to_string(*r))
                      : std::nullopt;
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        try {
          s.query_log_level = log_level_string_to_arrow_log_level(val);
        } catch (...) {
          return arrow::Status::Invalid("Invalid value for query_log_level: " + val);
        }
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession& s,
                       const std::string& val) -> arrow::Status {
        arrow::util::ArrowLogLevel lvl;
        try {
          lvl = log_level_string_to_arrow_log_level(val);
        } catch (...) {
          return arrow::Status::Invalid("Invalid value for query_log_level: " + val);
        }
        return srv.SetQueryLogLevel(s, lvl);
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.capture_query_profile",
      .scope = SetScopeKind::kSessionOrGlobal,
      .enterprise = true,
      .enterprise_feature = "instrumentation",
      .input_type = "VARCHAR",
      .env_var = "GIZMOSQL_CAPTURE_QUERY_PROFILE",
      .default_value = "off",
      .description =
          "Capture DuckDB query profiles into instrumentation "
          "(off/standard/detailed).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.capture_query_profile
                   ? std::optional(query_profile_mode_to_string(*s.capture_query_profile))
                   : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession& s) -> std::optional<std::string> {
        return query_profile_mode_to_string(srv.GetCaptureQueryProfile(s));
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        try {
          s.capture_query_profile = query_profile_mode_from_string(val);
        } catch (const std::invalid_argument& ex) {
          return arrow::Status::Invalid(ex.what());
        }
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession& s,
                       const std::string& val) -> arrow::Status {
        QueryProfileMode mode;
        try {
          mode = query_profile_mode_from_string(val);
        } catch (const std::invalid_argument& ex) {
          return arrow::Status::Invalid(ex.what());
        }
        return srv.SetCaptureQueryProfile(s, mode);
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.bypass_queue",
      .scope = SetScopeKind::kSessionOnly,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "BOOLEAN",
      .default_value = "false",
      .description = "Skip the statement queue for this session (admin only to enable).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.bypass_queue
                   ? std::optional(std::string(*s.bypass_queue ? "true" : "false"))
                   : std::nullopt;
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(bool b, ParseSetBool(val, "bypass_queue"));
        // Only admins may *enable* bypass; a non-admin must not be able to lift
        // their own queueing (privilege-escalation footgun).
        if (b && s.role != "admin") {
          return arrow::Status::Invalid(
              "Only admin users may set gizmosql.bypass_queue = true");
        }
        s.bypass_queue = b;
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.session_tag",
      .scope = SetScopeKind::kSessionOnly,
      .enterprise = true,
      .enterprise_feature = "instrumentation",
      .input_type = "VARCHAR",
      .description = "JSON session tag recorded in instrumentation.",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.session_tag.empty() ? std::nullopt : std::optional(s.session_tag);
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        if (!val.empty() && !IsValidJSON(val)) {
          return arrow::Status::Invalid("Invalid JSON for session_tag: " + val);
        }
        s.session_tag = val;
#ifdef GIZMOSQL_ENTERPRISE
        if (s.instrumentation) {
          s.instrumentation->UpdateSessionTag(val);
        }
#endif
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.query_tag",
      .scope = SetScopeKind::kSessionOnly,
      .enterprise = true,
      .enterprise_feature = "instrumentation",
      .input_type = "VARCHAR",
      .description = "JSON query tag recorded in instrumentation.",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.query_tag.empty() ? std::nullopt : std::optional(s.query_tag);
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        if (!val.empty() && !IsValidJSON(val)) {
          return arrow::Status::Invalid("Invalid JSON for query_tag: " + val);
        }
        s.query_tag = val;
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.max_concurrent_statements",
      .scope = SetScopeKind::kGlobalOnly,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_MAX_CONCURRENT_STATEMENTS",
      .default_value = "0",
      .description = "Max concurrently executing statements (0 = unlimited).",
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(std::to_string(srv.GetAdmissionController().Limit()));
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_concurrent_statements"));
        if (n < 0) return arrow::Status::Invalid("max_concurrent_statements must be >= 0");
        srv.GetAdmissionController().SetLimit(n);
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.max_queued_statements",
      .scope = SetScopeKind::kGlobalOnly,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_MAX_QUEUED_STATEMENTS",
      .default_value = "0",
      .description = "Max statements that may wait for a slot (0 = unbounded).",
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(std::to_string(srv.GetAdmissionController().MaxQueued()));
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_queued_statements"));
        srv.GetAdmissionController().SetMaxQueued(n);
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.max_queue_wait",
      .scope = SetScopeKind::kSessionOrGlobal,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_MAX_QUEUE_WAIT",
      .default_value = "300",
      .description =
          "Seconds a statement may wait in the queue before rejection (0 = forever).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.max_queue_wait ? std::optional(std::to_string(*s.max_queue_wait))
                                : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(
            std::to_string(srv.GetAdmissionController().DefaultMaxQueueWaitSeconds()));
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_queue_wait"));
        s.max_queue_wait = n;
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_queue_wait"));
        srv.GetAdmissionController().SetDefaultMaxQueueWaitSeconds(n);
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.graceful_shutdown",
      .scope = SetScopeKind::kGlobalOnly,
      .input_type = "BOOLEAN",
      .env_var = "GIZMOSQL_GRACEFUL_SHUTDOWN",
      .default_value = "false",
      .description =
          "Drain in-flight queries on SIGINT/SIGTERM instead of stopping "
          "immediately. Live-adjustable; takes effect on the next shutdown signal.",
      .get_global = [](DuckDBFlightSqlServer&,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(
            std::string(gizmosql::GracefulShutdownEnabled() ? "true" : "false"));
      },
      .set_global = [](DuckDBFlightSqlServer&, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(bool b, ParseSetBool(val, "graceful_shutdown"));
        gizmosql::g_graceful_enabled.store(b, std::memory_order_release);
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.shutdown_grace_period_seconds",
      .scope = SetScopeKind::kGlobalOnly,
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_SHUTDOWN_GRACE_PERIOD_SECONDS",
      .default_value = "300",
      .description =
          "Max seconds to wait for in-flight queries to drain during graceful "
          "shutdown (0 = wait indefinitely). Live-adjustable, including mid-drain.",
      .get_global = [](DuckDBFlightSqlServer&,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(
            std::to_string(gizmosql::GracefulShutdownGracePeriodSeconds()));
      },
      .set_global = [](DuckDBFlightSqlServer&, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n,
                              ParseSetInt(val, "shutdown_grace_period_seconds"));
        if (n < 0)
          return arrow::Status::Invalid("shutdown_grace_period_seconds must be >= 0");
        gizmosql::g_grace_period_seconds.store(n, std::memory_order_release);
        return arrow::Status::OK();
      },
  });

  for (size_t i = 0; i < settings_.size(); ++i) {
    by_name_[settings_[i].name] = i;
  }
}

const char* ScopeToString(SetScopeKind scope) {
  switch (scope) {
    case SetScopeKind::kSessionOnly:
      return "SESSION";
    case SetScopeKind::kGlobalOnly:
      return "GLOBAL";
    case SetScopeKind::kSessionOrGlobal:
      return "SESSION_OR_GLOBAL";
  }
  return "";
}

// Build the bind-parameterized VALUES that replaces a gizmosql_settings() call:
// "(VALUES (CAST(? AS ...),...),...) AS gizmosql_settings(<cols>)". Each value is
// appended to `binds` (row-major) and passed as a bound parameter — never
// interpolated — so JSON tags / descriptions can't break or inject SQL. The
// explicit CASTs fix the column types so GetSchema() works before binding.
std::string BuildGizmoSettingsValues(const ClientSession& session,
                                     DuckDBFlightSqlServer* server,
                                     duckdb::vector<duckdb::Value>& binds) {
  static constexpr const char* kRow =
      "(CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),"
      "CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),"
      "CAST(? AS BOOLEAN),CAST(? AS VARCHAR))";

  auto push = [&](const std::string& v) { binds.push_back(duckdb::Value(v)); };
  auto push_opt = [&](const std::optional<std::string>& v) {
    binds.push_back(v ? duckdb::Value(*v) : duckdb::Value());  // NULL when unset
  };

  std::string rows;
  const auto& settings = SettingsRegistry::Instance().All();
  for (size_t r = 0; r < settings.size(); ++r) {
    const GizmoSetting& d = settings[r];
    std::optional<std::string> sess = d.get_session ? d.get_session(session) : std::nullopt;
    std::optional<std::string> glob =
        (d.get_global && server) ? d.get_global(*server, session) : std::nullopt;
    std::string effective = sess.value_or(glob.value_or(d.default_value));

    push(d.name);                  // name
    push(effective);               // value (effective: session > global > default)
    push_opt(sess);                // session_value
    push_opt(glob);                // global_value
    push(ScopeToString(d.scope));  // scope
    push(d.input_type);            // input_type
    push(d.default_value);         // default_value
    push_opt(d.env_var.empty() ? std::optional<std::string>()
                               : std::optional<std::string>(d.env_var));  // env_var
    binds.push_back(duckdb::Value::BOOLEAN(d.enterprise));                // enterprise
    push(d.description);                                                  // description

    if (r) rows += ", ";
    rows += kRow;
  }

  return "(VALUES " + rows +
         ") AS gizmosql_settings(name, value, session_value, global_value, scope, "
         "input_type, default_value, env_var, enterprise, description)";
}

// Case-insensitively replace each "gizmosql_settings()" token with the
// bind-parameterized VALUES, appending its binds (in left-to-right order).
std::string RewriteGizmoSettings(const std::string& sql, const ClientSession& session,
                                 DuckDBFlightSqlServer* server,
                                 duckdb::vector<duckdb::Value>& binds) {
  static const std::string token = "gizmosql_settings()";
  const std::string lower = boost::algorithm::to_lower_copy(sql);
  std::string out;
  size_t pos = 0;
  while (true) {
    const size_t hit = lower.find(token, pos);
    if (hit == std::string::npos) {
      out += sql.substr(pos);
      break;
    }
    out += sql.substr(pos, hit - pos);
    out += BuildGizmoSettingsValues(session, server, binds);
    pos = hit + token.size();
  }
  return out;
}

}  // namespace

arrow::Status DuckDBStatement::HandleGizmoSQLSet() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  duckdb::Parser parser;
  try {
    parser.ParseQuery(sql_);
  } catch (const std::exception& ex) {
    return arrow::Status::Invalid("Failed to parse GizmoSQL SET command: " + sql_ +
                                  " - " + ex.what());
  }

  if (parser.statements.empty() ||
      parser.statements[0]->type != duckdb::StatementType::SET_STATEMENT) {
    return arrow::Status::Invalid("Expected SET statement: " + sql_);
  }

  auto& set_stmt = (duckdb::SetVariableStatement&)*parser.statements[0];

  const std::string& name = set_stmt.name;
  auto scope = set_stmt.scope;

  if (!set_stmt.value) {
    return Status::Invalid("SET requires a value");
  }
  std::string val;
  if (auto* const_expr =
          dynamic_cast<duckdb::ConstantExpression*>(set_stmt.value.get())) {
    val = const_expr->value.ToString();
  } else {
    // Bare keyword/identifier values (notably `= true` / `= false`) are not
    // represented as a ConstantExpression in DuckDB's grammar; fall back to the
    // parsed expression's textual form so booleans work without quoting.
    val = set_stmt.value->ToString();
  }

  if (!boost::istarts_with(name, "gizmosql.")) {
    return arrow::Status::Invalid("Unsupported GizmoSQL parameter: " + name);
  }

  // Dispatch through the settings registry (single source of truth). It performs
  // the enterprise-license, scope-validity, and GLOBAL-admin checks centrally and
  // invokes the per-setting mutator hook.
  ARROW_RETURN_NOT_OK(SettingsRegistry::Instance().Apply(
      *session, GetServer(*session).get(), name, scope, val));

  std::string scope_str = (scope == duckdb::SetScope::GLOBAL) ? "global" : "session";
  std::string msg =
      "GizmoSQL " + scope_str + " parameter '" + name + "' successfully set to: " + val;

  auto schema = arrow::schema({arrow::field("result", arrow::utf8())});
  arrow::StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append(msg));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  synthetic_result_batch_ = arrow::RecordBatch::Make(
      schema, 1, std::vector<std::shared_ptr<arrow::Array>>{array});
  return arrow::Status::OK();
}

DuckDBStatement::~DuckDBStatement() {
  // The plan cache's lease returns once this statement's own holders of the plan -
  // its result and its PreparedStatement - are released.
  if (PlanCacheEnabled()) {
    PlanLease lease;
    ForgetStatement(this, lease);
    if (lease.entry) {
      query_result_.reset();
      stmt_.reset();
      if (AsyncTeardownEnabled() && client_context_) {
        lease.db = client_context_->db;  // the lease's clear hands its aggregate states on
      }
    }
  }
}

DuckDBStatement::DuckDBStatement(const std::shared_ptr<ClientSession>& client_session,
                                 const std::string& handle,
                                 const std::shared_ptr<duckdb::PreparedStatement>& stmt,
                                 const std::optional<arrow::util::ArrowLogLevel>& log_level,
                                 const bool& log_queries,
                                 const std::shared_ptr<arrow::Schema>& override_schema,
                                 bool is_internal,
                                 std::string flight_method) {
  client_session_ = client_session;
  session_id_ = client_session->session_id;
  statement_id_ = handle;
  stmt_ = stmt;
  log_queries_ = log_queries;
  logged_sql_ = redact_sql_for_logs(stmt->query);
  use_direct_execution_ = false;
  log_level_ = log_level;
  is_internal_ = is_internal;
  flight_method_ = std::move(flight_method);
  start_time_ = std::chrono::steady_clock::now();
  override_schema_ = override_schema;
  query_result_ = nullptr;
  client_context_ = stmt->context;
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (auto trace_ids = GetCurrentTraceCorrelationIds()) {
    creation_trace_id_ = trace_ids->trace_id;
    creation_span_id_ = trace_ids->span_id;
  }
#endif
}

DuckDBStatement::DuckDBStatement(const std::shared_ptr<ClientSession>& client_session,
                                 const std::string& handle, const std::string& sql,
                                 const std::optional<arrow::util::ArrowLogLevel>& log_level,
                                 const bool& log_queries,
                                 const std::shared_ptr<arrow::Schema>& override_schema,
                                 bool is_internal,
                                 std::string flight_method) {
  client_session_ = client_session;
  session_id_ = client_session->session_id;
  statement_id_ = handle;
  sql_ = sql;
  log_queries_ = log_queries;
  logged_sql_ = redact_sql_for_logs(sql);
  use_direct_execution_ = true;
  stmt_ = nullptr;
  log_level_ = log_level;
  is_internal_ = is_internal;
  flight_method_ = std::move(flight_method);
  start_time_ = std::chrono::steady_clock::now();
  override_schema_ = override_schema;
  query_result_ = nullptr;
  client_context_ = client_session->connection->Get().context;
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (auto trace_ids = GetCurrentTraceCorrelationIds()) {
    creation_trace_id_ = trace_ids->trace_id;
    creation_span_id_ = trace_ids->span_id;
  }
#endif
}

arrow::Result<int> DuckDBStatement::Execute() {
  // Every execution of a statement that is not cacheable is inside a
  // generation pair, closed on every way out (a cancel and an exception included).
  GenerationPair plan_pair(PlanCacheEnabled() && !StatementCacheable(this));

  // The attached Flight call (if any) is only valid for this Execute(); never
  // let a later Execute() on a reused (prepared) statement see a stale one.
  struct ClearCallContext {
    const arrow::flight::ServerCallContext*& ctx;
    ~ClearCallContext() { ctx = nullptr; }
  } clear_call_context{call_context_};

  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());
  CountSessionStatement(session->session_id);

  // Mark the session busy for the idle-session sweeper for exactly the
  // duration of execution, on every exit path (RAII).
  ScopedSqlInFlight sql_in_flight_guard(session);

  std::string execute_status;

  ARROW_ASSIGN_OR_RAISE(auto query_timeout, GetQueryTimeout());
  // Threshold: session/server log level gates whether messages are emitted
  ARROW_ASSIGN_OR_RAISE(auto log_threshold, GetSessionOrServerLogLevel(session));
  // Display severity: statement's own level, defaulting to INFO for user queries
  auto log_level =
      log_level_.value_or(arrow::util::ArrowLogLevel::ARROW_INFO);
  start_time_ = std::chrono::steady_clock::now();

  const std::string metric_operation =
      is_gizmosql_admin_
          ? "ADMIN"
          : GetSqlOperationForMetrics(
                use_direct_execution_ ? sql_ : (stmt_ ? stmt_->query : sql_));
  auto record_query_metric = [this, &metric_operation](const std::string& status_label) {
    end_time_ = std::chrono::steady_clock::now();
    ::gizmosql::metrics::RecordQueryExecution(
        metric_operation, status_label, static_cast<double>(GetLastExecutionDurationMs()));
  };

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation execute_log_correlation(creation_trace_id_, creation_span_id_);
#endif

  // Generate execution ID for tracing (matches instrumentation table)
  std::string execution_id = NewUuid();

#ifdef GIZMOSQL_ENTERPRISE
  // Serialize bind parameters for instrumentation
  std::string bind_params_str;
  if (!bind_parameters.empty()) {
    std::stringstream params_ss;
    params_ss << "[";
    for (size_t i = 0; i < bind_parameters.size(); i++) {
      if (i > 0) params_ss << ", ";
      params_ss << "\"" << bind_parameters[i].ToString() << "\"";
    }
    params_ss << "]";
    bind_params_str = params_ss.str();
  }

  // Create execution instrumentation
  if (instrumentation_) {
    if (auto server = GetServer(*session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        execution_instrumentation_ = std::make_unique<ExecutionInstrumentation>(
            mgr, execution_id, statement_id_, bind_params_str);
      }
    }
  }

  // Configure DuckDB query profiling on this connection's ClientContext. We only
  // bother when there is an instrumentation record to store the profile into.
  // Profiling state is per-ClientContext and re-applied every Execute(), so a
  // session toggling the setting on/off is self-correcting. Capture is harvested
  // synchronously after execution (below), before the next statement on this
  // connection clobbers the profiler.
  //
  // Cross-version note (LTS v1.4 + stable v1.5): we touch only the ClientConfig
  // flags that exist in both channels and never reassign profiler_settings or call
  // MetricsUtils (whose API differs by version) — the default metric set is
  // version-correct, and enable_detailed_profiling is what enriches the tree.
  query_profile_mode_ = QueryProfileMode::kOff;
  if (execution_instrumentation_ && client_context_) {
    query_profile_mode_ = GetSessionOrServerCaptureProfile(session);
    auto& cfg = duckdb::ClientConfig::GetConfig(*client_context_);
    if (query_profile_mode_ == QueryProfileMode::kOff) {
      cfg.enable_profiler = false;
      cfg.enable_detailed_profiling = false;
    } else {
      cfg.enable_profiler = true;
      cfg.emit_profiler_output = false;  // no console/file output; harvested via ToJSON()
      cfg.profiler_print_format = duckdb::ProfilerPrintFormat::JSON;
      cfg.enable_detailed_profiling =
          (query_profile_mode_ == QueryProfileMode::kDetailed);
    }
  }
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Execute", execute_status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
      {"execution_id", execution_id},
      {"timeout_seconds", std::to_string(query_timeout)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (is_gizmosql_admin_) {
    // If we have a synthetic result (e.g., from KILL SESSION), execution is already done
    if (synthetic_result_batch_) {
#ifdef GIZMOSQL_ENTERPRISE
      // Mark execution as complete for admin commands
      if (execution_instrumentation_) {
        execution_instrumentation_->SetCompleted();
      }
#endif
      record_query_metric("OK");
      execute_status = "success";
      return 0;
    }
    auto set_status = HandleGizmoSQLSet();
    if (!set_status.ok()) {
#ifdef GIZMOSQL_ENTERPRISE
      // Mark execution as failed for SET command errors
      if (execution_instrumentation_) {
        execution_instrumentation_->SetError(set_status.ToString());
      }
#endif
      record_query_metric(set_status.CodeAsString());
      execute_status = "failure";
      return set_status;
    }
#ifdef GIZMOSQL_ENTERPRISE
    // Mark execution as complete for successful SET commands
    if (execution_instrumentation_) {
      execution_instrumentation_->SetCompleted();
    }
#endif
    record_query_metric("OK");
    execute_status = "success";
    return 0;
  }

  // ---- Statement-queue admission control (Enterprise; fails open) ----------
  // Acquire a concurrency slot for the duration of execution. DuckDB materializes
  // results inside Execute(), so this scope covers the heavy CPU/memory work; the
  // cheap FetchResult() iteration afterward needs no slot. Internal/metadata
  // queries are exempt. Without an enterprise license (or when the cap is 0),
  // Acquire() returns an inert handle (unlimited concurrency). The slot is held
  // until this function returns, covering the success, timeout, and error paths.
  // admission_server is declared first so it outlives admission_slot (the slot's
  // destructor releases back into the controller the server owns).
  std::shared_ptr<DuckDBFlightSqlServer> admission_server;
  gizmosql::AdmissionSlot admission_slot;
  {
    bool enforce_queue = false;
#ifdef GIZMOSQL_ENTERPRISE
    enforce_queue =
        !is_internal_ && !session->bypass_queue.value_or(false) &&
        gizmosql::enterprise::EnterpriseFeatures::Instance().IsFeatureAvailable(
            gizmosql::enterprise::kFeatureStatementQueue);
#endif
    if (enforce_queue) {
      admission_server = GetServer(*session);
      if (admission_server) {
        auto& controller = admission_server->GetAdmissionController();
        const int32_t max_queue_wait =
            session->max_queue_wait.value_or(controller.DefaultMaxQueueWaitSeconds());
#ifdef GIZMOSQL_ENTERPRISE
        // Record the queued phase (status='queued', enqueue_time) so the admin
        // SQL-monitor can show in-flight queued statements and the queue wait.
        if (execution_instrumentation_) {
          execution_instrumentation_->SetQueued();
        }
#endif
        // Pass an abort predicate so that if this session is killed while the
        // statement is queued, KILL SESSION's WakeWaiters() lets it abandon the
        // queue immediately (Cancelled) instead of waiting for a slot it will
        // never use.
        auto slot_result = controller.Acquire(
            /*enforce=*/true, max_queue_wait,
            /*is_aborted=*/[kr = &session->kill_requested] { return kr->load(); });
        if (!slot_result.ok()) {
          // Cancelled => the session was killed while we were queued: record a
          // cancellation and surface a Flight CANCELLED. Otherwise the queue was
          // full or the wait elapsed: surface a retriable Flight UNAVAILABLE so
          // clients can back off and retry.
          const bool killed =
              slot_result.status().code() == arrow::StatusCode::Cancelled;
#ifdef GIZMOSQL_ENTERPRISE
          if (execution_instrumentation_) {
            if (killed) {
              execution_instrumentation_->SetCancelled();
            } else {
              execution_instrumentation_->SetError(slot_result.status().message());
            }
          }
#endif
          return arrow::flight::MakeFlightError(
              killed ? arrow::flight::FlightStatusCode::Cancelled
                     : arrow::flight::FlightStatusCode::Unavailable,
              slot_result.status().message());
        }
        admission_slot = std::move(slot_result).ValueOrDie();
        // A kill can race the slot grant: if we were killed just as we were
        // admitted, don't execute the doomed statement — record it cancelled and
        // bail (the slot releases via admission_slot's destructor on return).
        if (session->kill_requested.load()) {
#ifdef GIZMOSQL_ENTERPRISE
          if (execution_instrumentation_) {
            execution_instrumentation_->SetCancelled();
          }
#endif
          return arrow::flight::MakeFlightError(
              arrow::flight::FlightStatusCode::Cancelled,
              "Statement cancelled: session was killed");
        }
#ifdef GIZMOSQL_ENTERPRISE
        // Slot acquired: queued -> executing (restarts the execution clock).
        if (execution_instrumentation_) {
          execution_instrumentation_->SetRunning();
        }
#endif
      }
    }
  }

  // Capture the current runtime context so trace/log correlation is preserved
  // when statement execution runs on the async worker thread.
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  auto telemetry_context = opentelemetry::context::RuntimeContext::GetCurrent();
#endif

  // Launch execution in a separate thread
  auto future = LaunchStatement(
      std::launch::async, [this, session, query_timeout, log_threshold, log_level
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
                           ,
                           telemetry_context,
                           statement_trace_id = creation_trace_id_,
                           statement_span_id = creation_span_id_
#endif
  ]() -> arrow::Result<int> {
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
        ScopedLogCorrelation async_log_correlation(statement_trace_id, statement_span_id);
        auto telemetry_context_token =
            opentelemetry::context::RuntimeContext::Attach(telemetry_context);
        (void)telemetry_context_token;
#endif
        if (use_direct_execution_) {
          // The statement may have already been executed from the ComputeSchema() method - if so, just skip execution
          if (query_result_ != nullptr) {
            if (log_queries_) {
              GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
                  log_threshold, log_level,
                  session,
                  "Direct execution of the SQL command has already occurred, skipping "
                  "re-execution",
                  {"kind", "sql"}, {"status", "already-executed"},
                  {"statement_id", statement_id_}, {"query_timeout", query_timeout},
                  {"is_internal", is_internal_ ? "true" : "false"},
                  {"flight_method", flight_method_});
            }
            return 0;  // Success
          }
          if (!bind_parameters.empty()) {
            session->active_sql_handle = "";
            return arrow::Status::Invalid(
                "Direct query execution does not support bind parameters");
          }

          auto result = session->connection->Get().Query(sql_);

          session->active_sql_handle = "";

          if (result->HasError()) {
            if (log_queries_) {
              GIZMOSQL_LOGKV_SESSION(
                  WARNING, session, "Client SQL command failed direct execution",
                  {"kind", "sql"}, {"status", "failure"},
                  {"statement_id", statement_id_}, {"error", result->GetError()},
                  {"sql", logged_sql_}, {"query_timeout", std::to_string(query_timeout)});
            }
            return arrow::Status::ExecutionError("Direct query execution error: ",
                                                 result->GetError());
          }

          query_result_ = std::move(result);
        } else {
          if (log_queries_ && !bind_parameters.empty()) {
            std::stringstream params_str;
            params_str << "[";
            for (size_t i = 0; i < bind_parameters.size(); i++) {
              if (i > 0) params_str << ", ";
              params_str << "'" << bind_parameters[i].ToString() << "'";
            }
            params_str << "]";

            GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
                log_threshold, log_level,
                session, "Executing prepared statement with bind parameters",
                {"kind", "sql"}, {"status", "executing"},
                {"statement_id", statement_id_}, {"bind_parameters", params_str.str()},
                {"param_count", std::to_string(bind_parameters.size())},
                {"query_timeout", std::to_string(query_timeout)},
                {"is_internal", is_internal_ ? "true" : "false"},
                {"flight_method", flight_method_});
          }

          // with the plan cache enabled, a statement that writes is executed
          // materialized, so its commit precedes its pair's post-bump
          if (PlanCacheEnabled() && !stmt_->data->properties.IsReadOnly()) {
            query_result_ = stmt_->Execute(bind_parameters, /*allow_stream_result=*/false);
          } else {
            query_result_ = stmt_->Execute(bind_parameters);
          }

          session->active_sql_handle = "";

          if (query_result_->HasError()) {
            if (log_queries_) {
              GIZMOSQL_LOGKV_SESSION(
                  WARNING, session, "Client SQL command failed execution",
                  {"kind", "sql"}, {"status", "failure"},
                  {"statement_id", statement_id_}, {"error", query_result_->GetError()},
                  {"sql", logged_sql_}, {"query_timeout", std::to_string(query_timeout)});
            }
            return arrow::Status::ExecutionError("An execution error has occurred: ",
                                                 query_result_->GetError());
          }
        }

        return 0;  // Success
      });
  WaitOnUnwind wait_on_unwind{future};

  std::future_status status;
  // Define timeout duration
  auto timeout_duration = std::chrono::seconds(query_timeout);

  // Wait for the execution thread, honouring the query timeout and — when a
  // Flight call is attached — the client going away. gRPC flags the call as
  // cancelled when the peer disconnects (killed process, dropped socket,
  // cancelled DoGet, client deadline); without polling for that here the
  // DuckDB query kept running with nobody left to receive the result.
  constexpr auto kClientGonePollInterval = std::chrono::milliseconds(100);
  const auto wait_deadline = std::chrono::steady_clock::now() + timeout_duration;
  bool client_gone = false;
  while (true) {
    status = future.wait_for(kClientGonePollInterval);
    if (status == std::future_status::ready) break;
    if (call_context_ != nullptr && call_context_->is_cancelled()) {
      client_gone = true;
      break;
    }
    if (query_timeout != 0 && std::chrono::steady_clock::now() >= wait_deadline) {
      status = std::future_status::timeout;
      break;
    }
  }

  if (client_gone) {
    if (log_queries_) {
      GIZMOSQL_LOGKV_SESSION(WARNING, session,
                             "Client went away during execution - interrupting statement",
                             {"kind", "sql"}, {"status", "canceled"},
                             {"reason", "client_disconnected"},
                             {"statement_id", statement_id_}, {"sql", logged_sql_},
                             {"flight_method", flight_method_});
    }

    session->connection->Get().Interrupt();
    future.wait();  // let the execution thread unwind cleanly
    session->active_sql_handle = "";

#ifdef GIZMOSQL_ENTERPRISE
    if (execution_instrumentation_) {
      execution_instrumentation_->SetCancelled();
    }
#endif
    record_query_metric("CANCELED");
    execute_status = "canceled";

    return arrow::Status::Cancelled("Query cancelled: client disconnected");
  }

  if (status == std::future_status::timeout) {
    if (log_queries_) {
      GIZMOSQL_LOGKV_SESSION(WARNING, session, "Client SQL command timed out - begin statement interruption",
                     {"kind", "sql"}, {"status", "timeout"},
                     {"interruption_status", "begin"},
                     {"statement_id", statement_id_},
                     {"timeout_seconds", std::to_string(query_timeout)},
                     {"sql", logged_sql_});
    }

    // Timeout occurred - interrupt the query
    session->connection->Get().Interrupt();

    // Now wait for the background thread to finish cleanly
    future.wait();

    session->active_sql_handle = "";

    if (log_queries_) {
      GIZMOSQL_LOGKV_SESSION(WARNING, session, "Client SQL command timed out - completed statement interruption",
                     {"kind", "sql"}, {"status", "timeout"},
                     {"interruption_status", "end"},
                     {"statement_id", statement_id_},
                     {"timeout_seconds", std::to_string(query_timeout)},
                     {"sql", logged_sql_});
    }

#ifdef GIZMOSQL_ENTERPRISE
    // Record timeout in instrumentation
    if (execution_instrumentation_) {
      execution_instrumentation_->SetTimeout();
    }
#endif
    record_query_metric("TIMEOUT");
    execute_status = "timeout";

    return arrow::Status::ExecutionError("Query execution timed out after ",
                                         std::to_string(timeout_duration.count()),
                                         " seconds");
  }

  // Get the result from the future
  auto result = future.get();

  end_time_ = std::chrono::steady_clock::now();

#ifdef GIZMOSQL_ENTERPRISE
  // Record success or error in instrumentation
  // Note: rows_fetched will be updated incrementally during FetchResult calls in the batch reader,
  // and the final record will be written when the ExecutionInstrumentation is destroyed
  if (execution_instrumentation_) {
    if (result.ok()) {
      // Harvest the DuckDB query profile while we still hold this connection and
      // before the next statement clobbers the per-ClientContext profiler. The
      // JSON is DuckDB's native profiling format, stored verbatim in
      // sql_executions.query_profile. Best-effort: never fail a query because
      // profiling threw, and only when capture is enabled for this execution.
      if (query_profile_mode_ != QueryProfileMode::kOff && client_context_) {
        try {
          std::string profile_json =
              duckdb::QueryProfiler::Get(*client_context_).ToJSON();
          execution_instrumentation_->SetQueryProfile(std::move(profile_json));
        } catch (const std::exception& ex) {
          GIZMOSQL_LOGKV_SESSION(WARNING, session,
                                 "Failed to capture query profile",
                                 {"statement_id", statement_id_},
                                 {"execution_id", execution_id}, {"error", ex.what()});
        }
      }
      execution_instrumentation_->SetCompleted();
    } else if (session->kill_requested) {
      // The query was interrupted by KILL SESSION, not a genuine execution error.
      execution_instrumentation_->SetCancelled();
    } else {
      execution_instrumentation_->SetError(result.status().ToString());
    }
  }
#endif

  if (log_queries_ && result.ok()) {
    GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
        log_threshold, log_level,
        session, "Client SQL command execution succeeded",
        {"kind", "sql"}, {"status", "success"}, {"statement_id", statement_id_},
        {"direct_execution", use_direct_execution_ ? "true" : "false"},
        {"duration_ms", GetLastExecutionDurationMs()}, {"sql", logged_sql_},
        {"is_internal", is_internal_ ? "true" : "false"},
        {"flight_method", flight_method_});
  }

  record_query_metric(result.ok() ? "OK" : result.status().CodeAsString());
  execute_status = result.ok() ? "success" : "failure";
  return result;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DuckDBStatement::FetchResult() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  // Fetch is still the same user query: refresh idle so a slow DoGet is not evicted.
  if (!is_internal_) {
    session->TouchSqlActivity();
  }

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation fetch_log_correlation(creation_trace_id_, creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::FetchResult", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (synthetic_result_batch_) {
    status = "success";
    auto batch = synthetic_result_batch_;
    synthetic_result_batch_.reset();
    if (batch && ::gizmosql::IsTelemetryEnabled()) {
      const auto batch_size_bytes = GetRecordBatchSizeBytes(batch);
      ::gizmosql::metrics::RecordBytesTransferred("outbound", batch_size_bytes);
      ::gizmosql::metrics::RecordRowsTransferred("outbound", batch->num_rows());
    }
    return batch;
  }

  std::shared_ptr<arrow::RecordBatch> record_batch;

  if (!query_result_) {
    // There is nothing to fetch...
    status = "success";
    return record_batch;
  }

  ArrowArray res_arr;
  ArrowSchema res_schema;

  auto res_options = client_context_->GetClientProperties();
  res_options.time_zone = query_result_->client_properties.time_zone;

  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  ARROW_RETURN_NOT_OK(arrow::ExportSchema(*schema, &res_schema));

  // RAII guard: release the exported ArrowSchema if we exit before
  // ImportRecordBatch() takes ownership (which consumes both res_schema and
  // res_arr via the C Data Interface).
  auto schema_guard = ::gizmosql::MakeScopeGuard([&res_schema]() {
    if (res_schema.release) {
      res_schema.release(&res_schema);
    }
  });

  duckdb::unique_ptr<duckdb::DataChunk> data_chunk;
  duckdb::ErrorData fetch_error;
  auto fetch_success = query_result_->TryFetch(data_chunk, fetch_error);
  if (!fetch_success) {
    return arrow::Status::ExecutionError(fetch_error.Message());
  }

  if (data_chunk != nullptr) {
    auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
        *client_context_, query_result_->types);
    duckdb::ArrowConverter::ToArrowArray(*data_chunk, &res_arr, res_options,
                                         extension_type_cast);
    ARROW_ASSIGN_OR_RAISE(record_batch, arrow::ImportRecordBatch(&res_arr, &res_schema));
    schema_guard.Dismiss();  // ownership transferred to record_batch

    GIZMOSQL_LOGKV_SESSION(DEBUG, session, "Client RecordBatch Fetch",
                   {"kind", "fetch"}, {"status", "success"},
                   {"statement_id", statement_id_},
                   {"num_rows", std::to_string(record_batch->num_rows())},
                   {"num_columns", std::to_string(record_batch->num_columns())},
                   {"sql", logged_sql_});
    if (::gizmosql::IsTelemetryEnabled()) {
      const auto record_batch_size_bytes = GetRecordBatchSizeBytes(record_batch);
      ::gizmosql::metrics::RecordBytesTransferred("outbound", record_batch_size_bytes);
      ::gizmosql::metrics::RecordRowsTransferred("outbound", record_batch->num_rows());
    }
  }

  status = "success";
  return record_batch;
}

std::shared_ptr<duckdb::PreparedStatement> DuckDBStatement::GetDuckDBStmt() const {
  if (use_direct_execution_) {
    // Direct execution mode doesn't have a prepared statement
    return nullptr;
  }
  return stmt_;
}

arrow::Result<int64_t> DuckDBStatement::ExecuteUpdate() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation execute_update_log_correlation(creation_trace_id_,
                                                      creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::ExecuteUpdate", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
      {"direct_execution", use_direct_execution_ ? "true" : "false"}, );

  ARROW_RETURN_NOT_OK(Execute());
  ARROW_ASSIGN_OR_RAISE(auto result_batch, FetchResult());

  if (!result_batch) {
    status = "success";
    return 0;
  }

  // For DML statements, DuckDB returns a single BIGINT value with the number of
  // affected rows. This is represented as a RecordBatch with one row and one
  // column.
  if (result_batch->num_rows() == 1 && result_batch->num_columns() == 1 &&
      result_batch->column(0)->type_id() == arrow::Type::INT64) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, result_batch->column(0)->GetScalar(0));
    if (scalar->is_valid) {
      status = "success";
      return std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
    }
  }

  // Fallback to previous behavior for other cases.
  status = "success";
  return result_batch->num_rows();
}

arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBStatement::GetSchema() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation get_schema_log_correlation(creation_trace_id_, creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::GetSchema", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  // If there is an override schema - just return it and avoid computation...
  if (override_schema_) {
    status = "success";
    return override_schema_;
  }

  // Lazily compute & memoize schema exactly once
  std::call_once(schema_once_flag_, [this] {
    cached_schema_ = ComputeSchema();  // Store the Result<>
  });

  status = "success";
  return cached_schema_;
}

long DuckDBStatement::GetLastExecutionDurationMs() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_)
      .count();
}

std::string DuckDBStatement::GetSessionId() const {
  return session_id_;
}

arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBStatement::ComputeSchema() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation compute_schema_log_correlation(creation_trace_id_,
                                                      creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::ComputeSchema", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (is_gizmosql_admin_) {
    status = "success";
    return arrow::schema({arrow::field("result", arrow::utf8())});
  }

  if (use_direct_execution_) {
    // For direct execution, we need to execute the query to get schema information
    ARROW_RETURN_NOT_OK(Execute());
    auto client_properties = client_context_->GetClientProperties();

    ArrowSchema arrow_schema;
    duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, query_result_->types,
                                          query_result_->names, client_properties);

    auto return_value = arrow::ImportSchema(&arrow_schema);
    status = "success";
    return return_value;
  }

  // Traditional prepared statement schema retrieval
  auto names = stmt_->GetNames();
  auto types = stmt_->GetTypes();

  auto client_properties = client_context_->GetClientProperties();

  ArrowSchema arrow_schema;
  duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);

  auto return_value = arrow::ImportSchema(&arrow_schema);
  status = "success";
  return return_value;
}

arrow::Result<int32_t> DuckDBStatement::GetQueryTimeout() const {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  // First, try getting the value from the user's session
  if (session->query_timeout.has_value()) {
    return session->query_timeout.value();
  }
  // Fall-back to the server's setting if the session setting is not set...
  if (auto server = GetServer(*session)) {
    return server->GetQueryTimeout(*session);
  }
  return arrow::Status::Invalid("Unable to get server instance");
}

arrow::Result<arrow::util::ArrowLogLevel> DuckDBStatement::GetLogLevel() const {
  if (log_level_.has_value()) {
    return log_level_.value();
  }
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());
  return GetSessionOrServerLogLevel(session);
}

arrow::Result<std::shared_ptr<ClientSession>> DuckDBStatement::GetSession() const {
  auto session = client_session_.lock();
  if (!session) return arrow::Status::Invalid("Session expired");
  return session;
}

}  // namespace gizmosql::ddb
