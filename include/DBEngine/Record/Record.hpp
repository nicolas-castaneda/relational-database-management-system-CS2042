#ifndef RECORD_HPP
#define RECORD_HPP

// This file contins compile time constants and type definitions
#include <limits>
#include <ranges>
#include <spdlog/spdlog.h>
#ifdef __clang__
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#pragma clang diagnostic ignored "-Wglobal-constructors"
#endif

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <string>
#include <unordered_set>
#include <vector>

namespace DB_ENGINE {

struct Attribute {
  std::string name;
  std::string value;
  friend auto operator<=>(const Attribute &, const Attribute &) = default;
};

struct Index {

  std::string table;
  std::string attribute_name;

  // spaceship operator
  friend auto operator<=>(const Index &, const Index &) = default;
};

struct Type {

  enum types : char { BOOL = 'b', INT = 'i', FLOAT = 'f', VARCHAR = 'c' };

  using size_type = uint16_t;

  static constexpr size_type MAX_VARCHAR_SIZE =
      std::numeric_limits<size_type>::max();

  size_type size;
  types type;

  Type() = default;
  Type(const types &_type, const size_type &_size) : size(_size), type(_type) {}
  explicit Type(const types &_type) : type(_type) {

    switch (type) {
    case BOOL:
      this->size = sizeof(bool);
      break;
    case INT:
      this->size = sizeof(int);
      break;
    case FLOAT:
      this->size = sizeof(float);
      break;
    case VARCHAR:
      throw std::invalid_argument(
          "Type initialization from varchar must have a size");
    }
  }
  Type(const types &_type, const int &_size)
      : Type(_type, static_cast<size_type>(_size)) {}
  [[nodiscard]] auto to_string() const -> std::string {
    std::string str;
    switch (type) {
    case BOOL:
      str += "BOOL";
      break;
    case INT:
      str += "INT";
      break;
    case FLOAT:
      str += "FLOAT";
      break;
    case VARCHAR:
      str += "VARCHAR";
      break;
    }
    str += "\tSize: " + std::to_string(size) + '\n';
    return str;
  }
};

namespace KEY_LIMITS {

// Ignore exit-time-destructors warning
const Attribute MIN = {"MIN", "MIN"};
const Attribute MAX = {"MAX", "MAX"};

} // namespace KEY_LIMITS

struct Record {
  using size_type = std::size_t;

  enum class Status : bool { DELETED = false, OK = true };

  Record() = default;
  Status status = Status::OK;

  explicit Record(auto split_view) {
    for (auto field : split_view) {
      m_fields.emplace_back(field.begin(), field.end());
    }
  }

  std::vector<std::string> m_fields;
  [[nodiscard]] auto begin() const { return m_fields.begin(); }
  [[nodiscard]] auto end() const { return m_fields.end(); }

  auto write(std::fstream &file, const std::vector<Type> &types) const
      -> std::ostream &;
  auto read(std::fstream &file, const std::vector<Type> &types)
      -> std::istream &;
  friend auto operator<=>(const Record &, const Record &) noexcept = default;
};

// https://en.cppreference.com/w/cpp/container/unordered_set
struct RecordHash {
  auto operator()(const Record &record) const -> size_t {
    size_t h = 0;
    for (const auto &field : record) {
      for (const auto &c : field) {
        h ^= std::hash<char>{}(c) + 0x9e3779b9 + (h << 6) + (h >> 2);
      }
    }
    return h;
  }
};

using query_time_t = std::unordered_map<std::string, std::chrono::milliseconds>;

struct QueryResponse {
  std::vector<Record> records;
  query_time_t query_times;
};

inline auto stob(std::string str) -> bool {

  std::transform(str.begin(), str.end(), str.begin(),
                 [](unsigned char c) { return std::toupper(c); });

  static std::unordered_set<std::string> valid_strings = {
      "YES", "Y", "SI", "S", "V", "VERDADERO", "T", "TRUE"};
  return valid_strings.contains(str);
}

template <typename Func>
inline void cast_and_execute(Type::types type,
                             const std::string &attribute_value, Func func) {
  spdlog::info("Casting {} ", attribute_value);
  switch (type) {
  case Type::types::INT: {
    spdlog::info("Casting {} to int", attribute_value);
    int key_value = std::stoi(attribute_value);
    func(key_value);
    break;
  }
  case Type::types::FLOAT: {
    float key_value = std::stof(attribute_value);
    func(key_value);
    break;
  }
  case Type::types::BOOL: {
    bool key_value = stob(attribute_value);
    func(key_value);
    break;
  }
  case Type::types::VARCHAR: {
    func(attribute_value);
    break;
  }
  }
}

template <typename Func>
inline void key_cast_and_execute(Type::types type,
                                 const std::string &attribute_value,
                                 Func func) {
  switch (type) {
  case Type::types::INT: {
    spdlog::info("Casting {} to int", attribute_value);
    int key_value = std::stoi(attribute_value);
    func(key_value);
    break;
  }
  case Type::types::FLOAT: {
    float key_value = std::stof(attribute_value);
    func(key_value);
    break;
  }
  case Type::types::BOOL: {
    spdlog::error("Bool key is not supported");
    break;
  }
  // case Type::types::VARCHAR: {
  //   func(attribute_value);
  //   break;
  // }
  // }
  case Type::types::VARCHAR:
    break;
  }
}

template <typename Func>
inline void cast_and_execute(Type::types type, const std::string &att1,
                             const std::string &att2, Func func) {
  switch (type) {
  case Type::types::INT: {
    int value_1 = std::stoi(att1);
    int value_2 = std::stoi(att2);
    func(value_1, value_2);
    break;
  }
  case Type::types::FLOAT: {
    float value_1 = std::stof(att1);
    float value_2 = std::stof(att2);
    func(value_1, value_2);
    break;
  }
  case Type::types::BOOL: {
    bool value_1 = stob(att1);
    bool value_2 = stob(att2);
    func(value_1, value_2);
    break;
  }
  case Type::types::VARCHAR: {
    func(att1, att2);
    break;
  }
  }
}

template <typename Func>
inline void key_cast_and_execute(Type::types type, const std::string &att1,
                                 const std::string &att2, Func func) {
  switch (type) {
  case Type::types::INT: {
    int value_1 = std::stoi(att1);
    int value_2 = std::stoi(att2);
    func(value_1, value_2);
    break;
  }
  case Type::types::FLOAT: {
    float value_1 = std::stof(att1);
    float value_2 = std::stof(att2);
    func(value_1, value_2);
    break;
  }
  case Type::types::BOOL: {
    spdlog::error("Bool key is not supported");
    break;
  }
  case Type::types::VARCHAR: {
    func(att1, att2);
    break;
  }
  }
}

} // namespace DB_ENGINE

#endif // !RECORD_HPP
