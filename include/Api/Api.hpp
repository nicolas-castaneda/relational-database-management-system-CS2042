#ifndef API_HPP
#define API_HPP

#include "DBEngine.hpp"
#include "SqlParser.hpp"
#include "crow.h"

class Api {

public:
  Api();
  Api(const Api &) = delete;
  Api(Api &&) = delete;
  auto operator=(const Api &) -> Api & = delete;
  auto operator=(Api &&) -> Api & = delete;
  ~Api() = default;

  void run();
  void handle(crow::request &req, crow::response &res);
  static auto get_engine() -> DBEngine & { return m_sql_parser.get_engine(); }

private:
  crow::SimpleApp m_app;

  inline static SqlParser m_sql_parser = SqlParser();

  void set_routes();
  static auto parse_query(const crow::request &req) -> crow::response;
};

#endif // !API_HPP
