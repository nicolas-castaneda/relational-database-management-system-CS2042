#include <iostream>
#ifdef __clang__
#pragma clang diagnostic ignored "-Wunsafe-buffer-usage"
#endif

#include "Record/Record.hpp"
#include "Utils/gsl.hpp"
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <numeric>

using DB_ENGINE::Record;

auto Record::write(std::fstream &file, const std::vector<Type> &types) const
    -> std::ostream & {

  spdlog::info("Writing record to file");

  int64_t buffer_size = std::accumulate(
      types.begin(), types.end(), static_cast<uint8_t>(0),
      [](uint8_t sum, const Type &type) { return sum + type.size; });

  std::unique_ptr<char[]> tmp_buffer(
      new char[static_cast<uint64_t>(buffer_size)]);

  auto buffer_current_pos = 0;
  for (size_type i = 0; i < types.size(); ++i) {

    auto field_size = types.at(i).size;

    if (buffer_current_pos + field_size > buffer_size) {
      // Handle the error, e.g., throw an exception, return an error code, etc.
      throw std::runtime_error("Buffer overflow detected");
    }

    switch (types.at(i).type) {
    case Type::BOOL: {
      bool casted_val =
          stob(std::string(m_fields.at(i).begin(), m_fields.at(i).end()));
      std::memcpy(tmp_buffer.get() + buffer_current_pos,
                  reinterpret_cast<const char *>(&casted_val), field_size);
      break;
    }
    case Type::INT: {
      int casted_val =
          stoi(std::string(m_fields.at(i).begin(), m_fields.at(i).end()));
      std::memcpy(tmp_buffer.get() + buffer_current_pos,
                  reinterpret_cast<const char *>(&casted_val), field_size);
      break;
    }
    case Type::FLOAT: {
      float casted_val =
          stof(std::string(m_fields.at(i).begin(), m_fields.at(i).end()));
      std::memcpy(tmp_buffer.get() + buffer_current_pos,
                  reinterpret_cast<const char *>(&casted_val), field_size);
      break;
    }
    case Type::VARCHAR: {
      auto str = std::string(m_fields.at(i).begin(), m_fields.at(i).end());

      spdlog::info("writing string: {}", str);

      // str must be of size field_size
      str.resize(field_size, ' ');
      spdlog::info("writing trimmed string: {}", str);

      std::memcpy(tmp_buffer.get() + buffer_current_pos, str.data(),
                  field_size);
      break;
    }
    }

    buffer_current_pos += field_size;
  }

  auto &response = file.write(tmp_buffer.get(), buffer_size);
  return response;
}

auto Record::read(std::fstream &file, const std::vector<Type> &types)
    -> std::istream & {
  int64_t buffer_size = std::accumulate(
      types.begin(), types.end(), static_cast<uint8_t>(0),
      [](uint8_t sum, const Type &type) { return sum + type.size; });

  std::cout << "Reading record from file at pos: " << file.tellg() << '\n';

  std::unique_ptr<char[]> tmp_buffer(
      new char[static_cast<uint64_t>(buffer_size)]);

  auto &response = file.read(tmp_buffer.get(), buffer_size);

  if (!response) {
    return response;
  }

  auto curr_buffer_pos = 0;

  m_fields.resize(types.size());

  for (size_type i = 0; i < types.size(); ++i) {
    auto field_size = types.at(i).size;

    switch (types.at(i).type) {
    case Type::BOOL: {
      bool read_value;
      std::memcpy(reinterpret_cast<char *>(&read_value),
                  tmp_buffer.get() + curr_buffer_pos, field_size);
      spdlog::info("Read value: {}", read_value);
      std::string str = read_value ? "true" : "false";
      m_fields.at(i) = {str.begin(), str.end()};
      break;
    }
    case Type::INT: {
      int read_value;
      std::memcpy(reinterpret_cast<char *>(&read_value),
                  tmp_buffer.get() + curr_buffer_pos, field_size);
      spdlog::info("Read value: {}", read_value);
      std::string str = std::to_string(read_value);
      m_fields.at(i) = {str.begin(), str.end()};
      break;
    }
    case Type::FLOAT: {
      float read_value;
      std::memcpy(reinterpret_cast<char *>(&read_value),
                  tmp_buffer.get() + curr_buffer_pos, field_size);
      spdlog::info("Read value: {}", read_value);

      std::string str = std::to_string(read_value);
      m_fields.at(i) = {str.begin(), str.end()};

      break;
    }
    case Type::VARCHAR: {
      std::string read_value(field_size,
                             ' '); // Resize the string to field_size
      std::memcpy(read_value.data(), tmp_buffer.get() + curr_buffer_pos,
                  field_size);

      spdlog::info("Read value: {}", read_value);

      // stack-use-after-return HERE
      std::string val(read_value.begin(), read_value.end());
      m_fields.at(i) = val;

      break;
    }
    }

    curr_buffer_pos += field_size;
  }

  return response;
}
