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

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/flight/sql/server.h>

#include "gizmosql_logging.h"
#include "session_context.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::FlightCallOptions;
using arrow::flight::Ticket;
using arrow::flight::sql::FlightSqlClient;

namespace {

// The ticket form and limits duckdb_server.cpp uses
constexpr std::string_view kTicketHandlePrefix = "tkt-";
constexpr std::size_t kTicketHandleLength = 40;
constexpr std::size_t kTicketStatementsPerSession = 64;

struct AuthedClient {
  std::unique_ptr<FlightSqlClient> sql_client;
  FlightCallOptions call_options;
};

struct TicketPayload {
  std::string transaction_id;
  std::string handle;
  std::string query;
};

arrow::Result<AuthedClient> Connect(int port, const std::string& username,
                                    const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(
      auto client, arrow::flight::FlightClient::Connect(
                       location, arrow::flight::FlightClientOptions()));
  FlightCallOptions call_options;
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        client->AuthenticateBasicToken({}, username, password));
  call_options.headers.push_back(bearer);
  return AuthedClient{std::make_unique<FlightSqlClient>(std::move(client)),
                      std::move(call_options)};
}

arrow::Result<TicketPayload> DecodeTicket(const Ticket& ticket) {
  ARROW_ASSIGN_OR_RAISE(
      auto command,
      arrow::flight::sql::StatementQueryTicket::Deserialize(ticket.ticket));
  const auto divider = command.statement_handle.find(':');
  if (divider == std::string::npos) {
    return arrow::Status::Invalid("statement ticket has no transaction divider");
  }

  TicketPayload payload;
  payload.transaction_id = command.statement_handle.substr(0, divider);
  std::string remainder = command.statement_handle.substr(divider + 1);
  if (remainder.starts_with(kTicketHandlePrefix) &&
      remainder.size() > kTicketHandleLength &&
      remainder[kTicketHandleLength] == ':') {
    payload.handle = remainder.substr(0, kTicketHandleLength);
    payload.query = remainder.substr(kTicketHandleLength + 1);
  } else {
    payload.query = std::move(remainder);
  }
  return payload;
}

arrow::Result<std::string> ReadOneValue(FlightSqlClient& client,
                                        const FlightCallOptions& options,
                                        const Ticket& ticket) {
  ARROW_ASSIGN_OR_RAISE(auto reader, client.DoGet(options, ticket));
  ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
  if (table->num_rows() != 1 || table->num_columns() != 1) {
    return arrow::Status::Invalid("expected exactly one row and one column");
  }
  ARROW_ASSIGN_OR_RAISE(auto value, table->column(0)->GetScalar(0));
  return value->ToString();
}

arrow::Result<Ticket> LegacyTicket(const std::string& statement_handle) {
  ARROW_ASSIGN_OR_RAISE(
      auto serialized,
      arrow::flight::sql::CreateStatementQueryTicket(statement_handle));
  return Ticket{std::move(serialized)};
}

}  // namespace

class StatementTicketReuseFixture
    : public gizmosql::testing::ServerTestFixture<StatementTicketReuseFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "statement_ticket_reuse_test.db",
        .port = 31640,
        .health_port = 31641,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .print_queries = true,
        .query_log_level = arrow::util::ArrowLogLevel::ARROW_INFO,
    };
  }

  void SetUp() override {
    gizmosql::ClearLogSinks();
    gizmosql::LogConfig config;
    config.level = arrow::util::ArrowLogLevel::ARROW_DEBUG;
    gizmosql::InitLogging(config);
    gizmosql::SetLogLevel(arrow::util::ArrowLogLevel::ARROW_DEBUG);
    create_count_.store(0);
    success_count_.store(0);
    reused_count_.store(0);
    gizmosql::RegisterLogSink([this](const gizmosql::LogRecord& record) {
      if (record.message.starts_with("DuckDBStatement::Create - BEGIN")) {
        create_count_.fetch_add(1);
      }
      if (record.level == arrow::util::ArrowLogLevel::ARROW_INFO &&
          record.message == "Client SQL command execution succeeded" &&
          record.fields_json.find("\"status\":\"success\"") !=
              std::string::npos) {
        success_count_.fetch_add(1);
      }
      if (record.level == arrow::util::ArrowLogLevel::ARROW_INFO &&
          record.message == "Client is attempting to run a SQL command" &&
          record.fields_json.find("\"status\":\"reused\"") != std::string::npos) {
        reused_count_.fetch_add(1);
      }
    });
  }

  void TearDown() override { gizmosql::ClearLogSinks(); }

  int CreateCount() const { return create_count_.load(); }
  int SuccessCount() const { return success_count_.load(); }
  int ReusedCount() const { return reused_count_.load(); }

 private:
  std::atomic<int> create_count_{0};
  std::atomic<int> success_count_{0};
  std::atomic<int> reused_count_{0};
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<StatementTicketReuseFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<StatementTicketReuseFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<StatementTicketReuseFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<StatementTicketReuseFixture>::config_{};

TEST_F(StatementTicketReuseFixture, SingleCreatePerStatement) {
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, client.sql_client->Execute(client.call_options, "SELECT 42"));
  ASSERT_EQ(info->endpoints().size(), 1u);
  EXPECT_EQ(CreateCount(), 1);
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto value, ReadOneValue(*client.sql_client, client.call_options,
                               info->endpoints()[0].ticket));
  EXPECT_EQ(value, "42");
  EXPECT_EQ(CreateCount(), 1);
  EXPECT_EQ(SuccessCount(), 1);
  EXPECT_EQ(ReusedCount(), 1);
}

TEST_F(StatementTicketReuseFixture, LegacyTicketWithoutHandleFallsBack) {
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto ticket, LegacyTicket(":SELECT 43"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto value,
      ReadOneValue(*client.sql_client, client.call_options, ticket));
  EXPECT_EQ(value, "43");
  EXPECT_EQ(CreateCount(), 1);
}

TEST_F(StatementTicketReuseFixture, UnknownHandleFallsBack) {
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto ticket,
      LegacyTicket(
          ":tkt-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:SELECT 44"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto value,
      ReadOneValue(*client.sql_client, client.call_options, ticket));
  EXPECT_EQ(value, "44");
  EXPECT_EQ(CreateCount(), 1);
}

TEST_F(StatementTicketReuseFixture, HandleIsSingleUse) {
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, client.sql_client->Execute(client.call_options, "SELECT 45"));
  ASSERT_EQ(info->endpoints().size(), 1u);
  const auto& ticket = info->endpoints()[0].ticket;
  ASSERT_ARROW_OK_AND_ASSIGN(auto payload, DecodeTicket(ticket));
  ASSERT_TRUE(payload.handle.starts_with(kTicketHandlePrefix));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto first,
      ReadOneValue(*client.sql_client, client.call_options, ticket));
  EXPECT_EQ(first, "45");
  EXPECT_EQ(CreateCount(), 1);

  // The ticket handle is not a prepared-statement handle: the two namespaces never cross.
  arrow::flight::sql::PreparedStatement closed_ticket(
      client.sql_client.get(), payload.handle, nullptr, nullptr);
  EXPECT_FALSE(closed_ticket.Close(client.call_options).ok());

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto second,
      ReadOneValue(*client.sql_client, client.call_options, ticket));
  EXPECT_EQ(second, "45");
  EXPECT_EQ(CreateCount(), 2);
}

TEST_F(StatementTicketReuseFixture, CountCapEvictsOldest) {
  // Two sessions, each registering 65 tickets: a DoGet runs a statement in its session,
  // which makes that session's other tickets stale, so each session takes one ticket.
  ASSERT_ARROW_OK_AND_ASSIGN(auto evicting,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto keeping,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  std::vector<Ticket> evicting_tickets;
  std::vector<Ticket> keeping_tickets;
  for (auto* session : {&evicting, &keeping}) {
    auto& tickets = session == &evicting ? evicting_tickets : keeping_tickets;
    for (std::size_t i = 0; i <= kTicketStatementsPerSession; ++i) {
      ASSERT_ARROW_OK_AND_ASSIGN(
          auto info,
          session->sql_client->Execute(session->call_options,
                                       "SELECT " + std::to_string(i)));
      ASSERT_EQ(info->endpoints().size(), 1u);
      tickets.push_back(info->endpoints()[0].ticket);
    }
  }
  EXPECT_EQ(CreateCount(), 130);

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto first, ReadOneValue(*evicting.sql_client, evicting.call_options,
                               evicting_tickets.front()));
  EXPECT_EQ(first, "0");
  EXPECT_EQ(CreateCount(), 131);
  EXPECT_EQ(ReusedCount(), 0);

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto newest, ReadOneValue(*keeping.sql_client, keeping.call_options,
                                keeping_tickets.back()));
  EXPECT_EQ(newest, "64");
  EXPECT_EQ(CreateCount(), 131);
  EXPECT_EQ(ReusedCount(), 1);
}

TEST_F(StatementTicketReuseFixture, StatementBetweenFallsBack) {
  // Any statement the session runs after a GetFlightInfo makes that ticket's prepared
  // statement stale: its DoGet prepares again.
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto first, client.sql_client->Execute(client.call_options, "SELECT 48"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto second, client.sql_client->Execute(client.call_options, "SELECT 49"));
  EXPECT_EQ(CreateCount(), 2);

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto second_value, ReadOneValue(*client.sql_client, client.call_options,
                                      second->endpoints()[0].ticket));
  EXPECT_EQ(second_value, "49");
  EXPECT_EQ(CreateCount(), 2);
  EXPECT_EQ(ReusedCount(), 1);

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto first_value, ReadOneValue(*client.sql_client, client.call_options,
                                     first->endpoints()[0].ticket));
  EXPECT_EQ(first_value, "48");
  EXPECT_EQ(CreateCount(), 3);
  EXPECT_EQ(ReusedCount(), 1);
}

TEST_F(StatementTicketReuseFixture, SetBetweenBindsTheNewSetting) {
  // integer_division is bound when the statement is prepared: a statement prepared
  // before the SET and reused after it would return 3.5.
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, client.sql_client->Execute(client.call_options, "SELECT 7 / 2"));
  ASSERT_ARROW_OK(client.sql_client
                      ->ExecuteUpdate(client.call_options, "SET integer_division = true")
                      .status());
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto value, ReadOneValue(*client.sql_client, client.call_options,
                               info->endpoints()[0].ticket));
  EXPECT_EQ(value, "3");
  EXPECT_EQ(CreateCount(), 3);
  EXPECT_EQ(ReusedCount(), 0);
}

TEST_F(StatementTicketReuseFixture, TransactionTicketRoundTrip) {
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto transaction,
      client.sql_client->BeginTransaction(client.call_options));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      client.sql_client->Execute(client.call_options, "SELECT 46", transaction));
  ASSERT_EQ(info->endpoints().size(), 1u);
  ASSERT_ARROW_OK_AND_ASSIGN(auto payload,
                             DecodeTicket(info->endpoints()[0].ticket));
  EXPECT_EQ(payload.transaction_id, transaction.transaction_id());

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto value, ReadOneValue(*client.sql_client, client.call_options,
                               info->endpoints()[0].ticket));
  EXPECT_EQ(value, "46");
  ASSERT_ARROW_OK(client.sql_client->Commit(client.call_options, transaction));
}

TEST_F(StatementTicketReuseFixture, AnotherSessionFallsBack) {
  ASSERT_ARROW_OK_AND_ASSIGN(auto owner,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto other,
                             Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, owner.sql_client->Execute(owner.call_options, "SELECT 47"));
  ASSERT_EQ(info->endpoints().size(), 1u);
  const auto& ticket = info->endpoints()[0].ticket;
  EXPECT_EQ(CreateCount(), 1);

  // Another session's DoGet prepares from the ticket text and leaves the owner's entry.
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto foreign, ReadOneValue(*other.sql_client, other.call_options, ticket));
  EXPECT_EQ(foreign, "47");
  EXPECT_EQ(CreateCount(), 2);

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto own, ReadOneValue(*owner.sql_client, owner.call_options, ticket));
  EXPECT_EQ(own, "47");
  EXPECT_EQ(CreateCount(), 2);
  EXPECT_EQ(ReusedCount(), 1);
}
