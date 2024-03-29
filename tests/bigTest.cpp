

#include <crow/app.h>
#include <crow/http_response.h>
#include <gtest/gtest.h>

#include "Api/Api.hpp"
#include "DBEngine.hpp"

constexpr bool REMOVE_AFTER_TEST = false;

class ElRealTest : public ::testing::Test {
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

TEST_F(ElRealTest, create) {
  auto query =
      "CREATE TABLE " + TEST_TABLE +
      " (count double, id int primary key, name char(12), paso_bd bool);";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, insertSingle) {
  auto query =
      "INSERT INTO " + TEST_TABLE + " Values (3.5, 8, 'AARON', 'false');";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}
TEST_F(ElRealTest, insertMore) {
  auto query1 =
      "INSERT INTO " + TEST_TABLE + " Values (3.5, 2, 'NICOLAS', 'true');";
  auto query2 =
      "INSERT INTO " + TEST_TABLE + " Values (3.5, 11, 'JUAQUIN', 'false');";
  auto query3 =
      "INSERT INTO " + TEST_TABLE + " Values (3.5, 10, 'ENRIQUE', 'true');";
  auto query4 =
      "INSERT INTO " + TEST_TABLE + " Values (3.5, 9, 'RENATO', 'true');";
  auto response1 = test_request(query1);
  auto response2 = test_request(query2);
  auto response3 = test_request(query3);
  auto response4 = test_request(query4);
  EXPECT_EQ(response1.code, 200);
  EXPECT_EQ(response2.code, 200);
  EXPECT_EQ(response3.code, 200);
  EXPECT_EQ(response4.code, 200);
}

TEST_F(ElRealTest, selectAll) {
  auto query = "SELECT * FROM " + TEST_TABLE + ";";
  auto response = test_request(query);
  spdlog::info("{}", response.body);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, selectSomeOne) {
  auto query = "SELECT name FROM " + TEST_TABLE + ";";
  auto response = test_request(query);
  spdlog::info("{}", response.body);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, selectSomeMore) {
  auto query = "SELECT name, id FROM " + TEST_TABLE + ";";
  auto response = test_request(query);
  spdlog::info("{}", response.body);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, selectWhere) {
  auto query = "SELECT * FROM " + TEST_TABLE + " WHERE id = 8;";
  auto response = test_request(query);
  spdlog::info("{}", response.body);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, selectWithComplexCondition) {
  auto query =
      "SELECT FROM " + TEST_TABLE + " WHERE id > 5 AND name = 'AARON';";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, selectWithMoreComplexCondition) {
  auto query = "SELECT FROM " + TEST_TABLE +
               " WHERE id < 12 AND name = 'AARON' OR name = 'JUAQUIN';";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, selectWithEvenMoreComplexCondition) {
  auto query =
      "SELECT FROM " + TEST_TABLE +
      " WHERE id > 5 AND name = 'AARON' OR name = 'JUAQUIN' AND id < 12;";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, deleteErrorParser) {
  auto query = "DELETE FROM " + TEST_TABLE + ";";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 500);
}

TEST_F(ElRealTest, deleteFromTable) {
  auto query = "DELETE FROM " + TEST_TABLE + " WHERE id = 8;";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, deleteFromTableWithCondition) {
  auto query = "DELETE FROM " + TEST_TABLE + " WHERE id < 5;";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, deleteFromTableWithComplexCondition) {
  auto query = "DELETE FROM " + TEST_TABLE +
               " WHERE id > 5 AND name = 'JUAQUIN' OR name = 'ENRIQUE;";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, deleteFromNonExistentTable) {
  auto query = "DELETE FROM non_existent_table WHERE id = 8;";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 400);
}

TEST_F(ElRealTest, dropTable) {
  if (!REMOVE_AFTER_TEST) {
    return;
  }
  auto query = "DROP TABLE " + TEST_TABLE + ";";
  auto response = test_request(query);
  EXPECT_EQ(response.code, 200);
}

TEST_F(ElRealTest, tableDropped) {
  if (!REMOVE_AFTER_TEST) {
    return;
  }
  EXPECT_FALSE(engine.is_table(TEST_TABLE));
}
