#include <format>

#include <crow/app.h>
#include <crow/http_response.h>
#include <gtest/gtest.h>

#include "Api/Api.hpp"
#include "DBEngine.hpp"

class BulkInsertTest : public ::testing::Test {
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
  void TearDown() override { engine.drop_table(TEST_TABLE); }
};

TEST_F(BulkInsertTest, SimpleInsert) {

  const std::string TABLE_CREATE = std::format(
      "CREATE TABLE {}(id int primary key, nombre char(10), apellido "
      "char(20), aprobo_bd bool, score double);",
      TEST_TABLE);
  test_request(TABLE_CREATE);

  const std::string QUERY =
      std::format("insert into {} from 'students_insert';", TEST_TABLE);
  auto response_insert = test_request(QUERY);
  EXPECT_EQ(response_insert.code, 200);

  const std::string SELECT_QUERY = "SELECT * FROM test_table;";
  auto response_select = test_request(SELECT_QUERY);
  EXPECT_EQ(response_select.code, 200);
  spdlog::info("{}", response_select.body);
}
// TEST_F(BulkInsertTest, BigInsert) {
//   const std::string TABLE_CREATE =
//       "CREATE TABLE music(id int primary key, nombre char(12), num "
//       "int, score double);";
//   test_request(TABLE_CREATE);
//
//   const std::string QUERY =
//       std::format("insert into music from 'data2';", TEST_TABLE);
//   auto response_insert = test_request(QUERY);
//   EXPECT_EQ(response_insert.code, 200);
//
//   const std::string SELECT_QUERY = "SELECT * FROM music;";
//   auto response_select = test_request(SELECT_QUERY);
//   EXPECT_EQ(response_select.code, 200);
//   spdlog::info("{}", response_select.body);
// }
