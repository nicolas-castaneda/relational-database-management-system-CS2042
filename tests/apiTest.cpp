#include "Api/Api.hpp"
#include <crow/app.h>
#include <gtest/gtest.h>

class ApiTests : public ::testing::Test {
protected:
  Api app;
};

TEST_F(ApiTests, Status200) {

  crow::request req;
  crow::response res;

  req.url = "/query";
  req.method = crow::HTTPMethod::POST;
  req.body =
      R"({"query": "CREATE TABLE t_name(iden int primary key, col1 char(1), col2 char(4));"})";

  app.handle(req, res);
  EXPECT_EQ(res.code, 200);
}

TEST_F(ApiTests, Status400) {
  crow::request req;
  crow::response res;

  req.url = "/query";
  req.method = crow::HTTPMethod::POST;
  req.body = R"({"not_query": "SELECT * FROM users"})";

  app.handle(req, res);
  EXPECT_EQ(res.code, 400);
}

TEST_F(ApiTests, Status404) {

  crow::request req;
  crow::response res;

  req.url = "/other_path";
  req.method = crow::HTTPMethod::POST;
  req.body = R"({"not_query": "SELECT * FROM users"})";

  app.handle(req, res);
  EXPECT_EQ(res.code, 404);
}
TEST_F(ApiTests, Status405) {

  crow::request req;
  crow::response res;

  req.url = "/query";
  req.method = crow::HTTPMethod::GET;
  req.body = R"({"not_query": "SELECT * FROM users"})";

  app.handle(req, res);
  EXPECT_EQ(res.code, 405);
}
