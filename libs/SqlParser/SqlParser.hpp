#ifndef SQL_PARSER_HPP
#define SQL_PARSER_HPP

#include <concepts>
#include <cstddef>
#include <istream>
#include <string>
#include <unordered_set>
#include <vector>

#include "Record/Record.hpp"
#include "parser.tab.hh"
#include "scanner.hpp"

struct ParserResponse {
  std::string records;
  std::string query_times;
};

class SqlParser {
public:
  SqlParser() = default;

  ~SqlParser();

  void parse(const char *filename);

  auto parse(std::istream &stream) -> ParserResponse;

  void check_table_name(const std::string &tablename);

  void create_table(const std::string &tablename,
                    const std::vector<column_t> &columns);

  void create_index(const std::string &tablename,
                    const std::string &column_name,
                    const DB_ENGINE::DBEngine::Index_t &index_name);

  void select(const std::string &tablename,
              const std::vector<std::string> &column_names,
              const std::list<std::list<condition_t>> &constraints);
  void select_between(const std::string &tablename,
                      const std::vector<std::string> &column_names,
                      const std::string &id, const std::string &val1,
                      const std::string &val2);
  auto get_engine() -> DB_ENGINE::DBEngine & { return m_engine; }

  void insert_from_file(const std::string &tablename,
                        const std::string &filename);

  void insert(const std::string &tablename,
              const std::vector<std::string> &values);

  void remove(const std::string &tablename,
              std::list<std::list<condition_t>> &constraint);

  void drop_table(const std::string &tablename);

private:
  DB_ENGINE::DBEngine m_engine;
  ParserResponse m_parser_response;

  void query_to_json(const DB_ENGINE::QueryResponse &query_response,
                     const std::vector<std::string> &sorted_column_names);
  void parse_helper(std::istream &stream);
  std::unordered_set<std::string> m_tablenames;
  yy::parser *m_parser = nullptr;
  scanner *m_sc = nullptr;

  static auto merge_records(const std::vector<Record> &vec1,
                            const std::vector<Record> &vec2)
      -> std::vector<Record>;

  static auto merge_times(query_time_t &times_1, const query_time_t &times_2)
      -> query_time_t &;
};

#endif // SQL_PARSER_HPP
