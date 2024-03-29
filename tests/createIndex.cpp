#include <crow/app.h>
#include <crow/http_response.h>
#include <gtest/gtest.h>

#include "Api/Api.hpp"
#include "DBEngine.hpp"

constexpr bool REMOVE_AFTER_TEST = false;

class CreateIndex : public ::testing::Test {
protected:
  Api app;
  crow::request req;

  DBEngine &engine = Api::get_engine();

  std::string TEST_TABLE = "test_table";

  auto test_request(const std::string &query) -> crow::response {
    crow::response res;
    req.body = R"({"query": ")" + query + R"("})";
    app.handle(req, res);
    return res;
  }
  void SetUp() override {
    req.url = "/query";
    req.method = crow::HTTPMethod::POST;
  }
};

TEST_F(CreateIndex, create) {
  std::string query =
      "CREATE TABLE test(id int primary key, col1 char(50), mode int, "
      "val double);";

  std::string query2 = "INSERT INTO test FROM 'data2 copy';";
  std::string query4 = "select * from test where mode < 3;";

  auto r1 = test_request(query);
  auto r2 = test_request(query2);
  auto r4 = test_request(query4);
}

// TEST_F(CreateIndex, create) {
//   auto query = "CREATE TABLE " + TEST_TABLE +
//                " (count double, id int primary key, name char(12), paso_bd "
//                "bool, nu int);";
//   auto response = test_request(query);
//   EXPECT_EQ(response.code, 200);
// }
//
// TEST_F(CreateIndex, createIndexSequential1) {
//   auto query = "CREATE INDEX SEQ ON " + TEST_TABLE + "(nu);";
//   auto response = test_request(query);
//   EXPECT_EQ(response.code, 200);
// }
//
// TEST_F(CreateIndex, insertSingle) {
//   auto query =
//       "INSERT INTO " + TEST_TABLE + " Values (3.5, 8, 'qwerty', 'true',
//       45);";
//   auto response = test_request(query);
//   EXPECT_EQ(response.code, 200);
// }
//
// TEST_F(CreateIndex, insertMore) {
//   auto query1 =
//       "INSERT INTO " + TEST_TABLE + " Values (3.5, 10, 'qwerty', 'true',
//       93);";
//   auto query2 =
//       "INSERT INTO " + TEST_TABLE + " Values (3.5, 9, 'qwerty', 'true',
//       12);";
//   auto response1 = test_request(query1);
//   auto response2 = test_request(query2);
//   EXPECT_EQ(response1.code, 200);
//   EXPECT_EQ(response2.code, 200);
// }
//
// TEST_F(CreateIndex, createIndexSequential2) {
//   auto query = "CREATE INDEX SEQ ON " + TEST_TABLE + "(count);";
//   auto response = test_request(query);
//   EXPECT_EQ(response.code, 200);
// }
//
// TEST_F(CreateIndex, selectAll) {
//   auto query = "SELECT * FROM " + TEST_TABLE + ";";
//   auto response = test_request(query);
//   spdlog::info("{}", response.body);
//   EXPECT_EQ(response.code, 200);
// }
//
// TEST_F(CreateIndex, selectSome) {
//
//   auto query = "SELECT name, id FROM " + TEST_TABLE + ";";
//   auto response = test_request(query);
//   spdlog::info("{}", response.body);
//   EXPECT_EQ(response.code, 200);
// }
//
// TEST_F(CreateIndex, dropTable) {
//   if (!REMOVE_AFTER_TEST) {
//     return;
//   }
//   auto query = "DROP TABLE " + TEST_TABLE + ";";
//   auto response = test_request(query);
//   EXPECT_EQ(response.code, 200);
// }
//
// TEST_F(CreateIndex, tableDropped) {
//   if (!REMOVE_AFTER_TEST) {
//     return;
//   }
//   EXPECT_FALSE(engine.is_table(TEST_TABLE));
// }
