#include <algorithm>
#include <bitset>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>

#include <spdlog/spdlog.h>
#include <variant>

#include "Constants.hpp"
#include "DBEngine.hpp"

using DB_ENGINE::DBEngine;

using ::std::filesystem::create_directory;
using ::std::filesystem::exists;

constexpr float FLOAT_EPSILON = 0.001F;

static auto get_sample_value(DB_ENGINE::Type type) -> std::string {
  switch (type.type) {
  case DB_ENGINE::Type::BOOL:
    return "true";
  case DB_ENGINE::Type::INT:
    return "0";
  case DB_ENGINE::Type::FLOAT:
    return "0.0";
  case DB_ENGINE::Type::VARCHAR:
    return "a";
  }
  throw std::runtime_error("Invalid type");
}

DB_ENGINE::DBEngine::DBEngine() {
  generate_directories();

  // Read tables raw data
  for (auto const &dir_entry :
       std::filesystem::directory_iterator{TABLES_PATH}) {
    HeapFile heap_file(dir_entry.path());
  }

  // Load Indexes
}

auto DBEngine::create_table(const std::string &table_name,
                            const std::string &primary_key,
                            const std::vector<Type> &types,
                            const std::vector<std::string> &attribute_names)
    -> bool {

  // Check if table already exists
  std::string table_path = TABLES_PATH + table_name;

  if (exists(table_path)) {
    spdlog::warn("Table {} already exists doing nothing.", table_name);
    return false;
  }

  spdlog::info("Creating table {} in dir {}", table_name, table_path);

  create_directory(table_path);

  HeapFile heap_file(table_name, types, attribute_names, primary_key);

  m_tables_raw.insert({table_name, std::move(heap_file)});

  auto key_type = m_tables_raw.at(table_name).get_type(primary_key);

  switch (key_type.type) {
  case Type::INT: {
    SequentialIndex<int> sequential_index_int(table_name, primary_key, true);
    m_sequential_indexes.insert(
        {{table_name, primary_key},
         SequentialIndexContainer(sequential_index_int)});
    break;
  }
  case Type::FLOAT: {
    SequentialIndex<float> sequential_index_float(table_name, primary_key,
                                                  true);
    m_sequential_indexes.insert(
        {{table_name, primary_key},
         SequentialIndexContainer(sequential_index_float)});
    break;
  }
  case Type::VARCHAR: {
    SequentialIndex<std::string> sequential_index_str(table_name, primary_key,
                                                      true);
    m_sequential_indexes.insert(
        {{table_name, primary_key},
         SequentialIndexContainer(sequential_index_str)});
    break;
  }
  case Type::BOOL: {
    spdlog::error("Bool can't be indexed. at: table_creation");
  }
  }

  return true;
}

auto DBEngine::get_table_names() -> std::vector<std::string> {

  std::vector<std::string> table_names;

  for (const auto &table : m_tables_raw) {
    table_names.push_back(table.first);
  }
  return table_names;
}

auto DBEngine::search(const std::string &table_name, const Attribute &key,
                      const std::function<bool(Record)> &expr,
                      const std::vector<std::string> &selected_attributes)
    -> QueryResponse {

  std::vector<HeapFile::pos_type> positions;

  query_time_t times;

  auto key_type = m_tables_raw.at(table_name).get_type(key);

  key_cast_and_execute(key_type.type, key.value,
                       [this, &key, &times, &positions](auto key_value) {
                         for (const auto &idx : m_sequential_indexes) {
                           if (idx.second.get_attribute_name() == key.name) {

                             auto search_response =
                                 idx.second.search(key_value);
                             positions = search_response.first;
                             times["SEQUENTIAL_SINGLE_SEARCH"] =
                                 search_response.second;
                             break;
                           }
                         }
                       });

  std::vector<Record> response;

  for (const auto &pos : positions) {
    auto rec = m_tables_raw.at(table_name).read(pos);

    if (expr(rec)) {
      response.push_back(rec);
    }
  }
  return m_tables_raw.at(table_name)
      .filter(response, selected_attributes, times);
}
auto DBEngine::range_search(const std::string &table_name, Attribute begin_key,
                            Attribute end_key,
                            const std::function<bool(Record)> &expr,
                            const std::vector<std::string> &selected_attributes)
    -> QueryResponse {

  if (begin_key == KEY_LIMITS::MIN && end_key == KEY_LIMITS::MAX) {
    QueryResponse response = load(table_name, selected_attributes);

    response.records.erase(
        std::remove_if(response.records.begin(), response.records.end(),
                       [&expr](const Record &obj) { return !expr(obj); }),
        response.records.end());
  }

  if (begin_key.name != end_key.name) {

    if ((end_key != KEY_LIMITS::MIN && end_key != KEY_LIMITS::MAX) &&
        (begin_key != KEY_LIMITS::MIN && begin_key != KEY_LIMITS::MAX)) {
      throw std::runtime_error(
          "Cant apply range_search to different attributes");
    }
  }

  if (begin_key == KEY_LIMITS::MIN) {
    begin_key.value = std::to_string(std::numeric_limits<int>::min());
    begin_key.name = end_key.name;
  }
  if (end_key == KEY_LIMITS::MAX) {
    end_key.value = std::to_string(std::numeric_limits<int>::max());
    end_key.name = begin_key.name;
  }
  spdlog::info("Range search names: {}, {}", begin_key.name, end_key.name);
  spdlog::info("Range search values: {}, {}", begin_key.value, end_key.value);

  std::vector<HeapFile::pos_type> positions;
  query_time_t times;

  auto key_type = m_tables_raw.at(table_name).get_type(begin_key);

  key_cast_and_execute(
      key_type.type, begin_key.value, end_key.value,
      [this, &positions, &begin_key, &times](auto begin_key_value,
                                             auto end_key_value) {
        for (const auto &idx : m_sequential_indexes) {
          if (idx.second.get_attribute_name() == begin_key.name) {
            auto idx_response =
                idx.second.range_search(begin_key_value, end_key_value);
            positions = idx_response.records;
            times["SEQUENTIAL_RANGE_SEARCH"] = idx_response.query_time;
          }
        }
      });

  spdlog::info("Positions:");
  for (const auto &pos : positions) {
    spdlog::info("{}", static_cast<std::streamoff>(pos));
  }

  std::vector<Record> response;

  for (const auto &pos : positions) {
    auto rec = m_tables_raw.at(table_name).read(pos);

    if (expr(rec)) {
      response.push_back(rec);
    }
  }
  return m_tables_raw.at(table_name)
      .filter(response, selected_attributes, times);
}

auto DBEngine::load(const std::string &table_name,
                    const std::vector<std::string> &selected_attributes)
    -> QueryResponse {

  auto start = std::chrono::high_resolution_clock::now();
  auto response = m_tables_raw.at(table_name).load();
  auto end = std::chrono::high_resolution_clock::now();

  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  query_time_t times{{"LOAD", duration}};

  spdlog::info("Response size: {}", response.size());

  return m_tables_raw.at(table_name)
      .filter(response, selected_attributes, times);
}

auto DBEngine::add(const std::string &table_name,
                   const std::vector<std::string> &value) -> bool {

  spdlog::info("Calling add");

  Record rec(value);

  auto &table = m_tables_raw.at(table_name);

  auto [type, key] = table.get_key(rec);

  auto inserted_pos = table.next_pos();

  response_time time;

  bool pk_inserted = false;
  for (auto &idx : m_sequential_indexes) {
    if (idx.first.table == table_name && idx.first.attribute_name == key.name) {

      spdlog::info("value to cast: {}", key.value);

      key_cast_and_execute(
          type.type, key.value,
          [&idx, &inserted_pos, &pk_inserted, &time](auto key_val) {
            spdlog::info("adding to sequential: {} {}", key_val,
                         static_cast<std::streamoff>(inserted_pos));
            auto add_response = idx.second.add(key_val, inserted_pos);
            pk_inserted = add_response.first;
            time = add_response.second;
          });
    }
  }

  if (!pk_inserted) {
    spdlog::info("Record with key {} already exists", key.name);
    throw std::runtime_error("Record already exists");
  }

  // Insert whole record
  m_tables_raw.at(table_name).add(rec);

  // Insert into indexes

  return pk_inserted;
}

auto DBEngine::remove(const std::string &table_name, const Attribute &key)
    -> bool {

  response_time time;
  HeapFile::pos_type raw_pos;
  for (const auto &idx : m_sequential_indexes) {
    if (idx.first.table == table_name) {
      auto type = m_tables_raw.at(table_name).get_type(key);
      key_cast_and_execute(
          type.type, key.value, [&raw_pos, &idx, &time](auto key_value) {
            auto remove_response = idx.second.remove(key_value);
            raw_pos = remove_response.first;
            time = remove_response.second;
          });
    }
  }
  if (raw_pos == -1) {
    return false;
  }

  m_tables_raw.at(table_name).remove(raw_pos);
  return true;
}

void DBEngine::create_index(const std::string &table_name,
                            const std::string &column_name,
                            const Index_t &index_type) {

  auto record_size = m_tables_raw.at(table_name).get_record_size();
  auto pos_getter = [&record_size]() {
    // Starting pos
    static std::streampos prev_pos = -record_size;
    prev_pos += record_size;
    return prev_pos;
  };

  const auto TYPE = m_tables_raw.at(table_name).get_type(column_name);

  // type
  switch (index_type) {
  case Index_t::AVL: {
    spdlog::info("Creating avl index for {} {}", table_name, column_name);
    break;
  }
  case Index_t::ISAM: {
    spdlog::info("Creating isam index for {} {}", table_name, column_name);
    break;
  }
  case Index_t::SEQUENTIAL: {
    spdlog::info("Creating seq index for {} {}", table_name, column_name);
    break;
  }
  }

  if (TYPE.type == Type::BOOL) {
    spdlog::error("Bool can't be indexed. at: create_index");
    throw std::invalid_argument("Bool can't be indexed");
  }

  key_cast_and_execute(
      TYPE.type, get_sample_value(TYPE),
      [&index_type, &table_name, &column_name, this, &TYPE,
       &pos_getter](auto value) {
        using att_type = decltype(value);

        std::vector<std::pair<att_type, std::streampos>> key_values;
        auto all_records = load(table_name, {column_name}).records;
        std::transform(
            all_records.begin(), all_records.end(),
            std::back_inserter(key_values), [&TYPE, &pos_getter](auto record) {
              auto str_val = record.m_fields.at(0);
              auto str_value = std::string(str_val.begin(), str_val.end());

              std::pair<att_type, std::streampos> return_v;

              switch (TYPE.type) {

              case Type::BOOL:
              case Type::VARCHAR:
                break;

              case Type::INT:
                return_v = {std::stoi(str_value), pos_getter()};
                break;

              case Type::FLOAT:
                return_v = {std::stof(str_value), pos_getter()};
                break;
              }
              return return_v;
            });

        switch (index_type) {
        case Index_t::AVL: {

          if (m_avl_indexes.contains({table_name, column_name})) {
            spdlog::warn("Index already exists");
            throw std::invalid_argument("Index already exists");
          }
          AVLIndex<att_type> idx(table_name, column_name, false);
          m_avl_indexes.emplace(Index(table_name, column_name), idx);
          m_avl_indexes.at({table_name, column_name})
              .bulk_insert<att_type>(key_values);
          break;
        }
        case Index_t::SEQUENTIAL: {
          if (m_avl_indexes.contains({table_name, column_name})) {
            spdlog::warn("Index already exists");
            throw std::invalid_argument("Index already exists");
          }

          SequentialIndex<att_type> idx(table_name, column_name, false);
          m_sequential_indexes.emplace(Index(table_name, column_name), idx);
          m_sequential_indexes.at({table_name, column_name})
              .bulk_insert<att_type>(key_values);
          break;
        }
        case Index_t::ISAM: {
          if (m_avl_indexes.contains({table_name, column_name})) {
            spdlog::warn("Index already exists");
            throw std::invalid_argument("Index already exists");
          }
          ISAM<att_type> idx(table_name, column_name, false);
          m_isam_indexes.emplace(Index(table_name, column_name), idx);
          m_isam_indexes.at({table_name, column_name})
              .bulk_insert<att_type>(key_values);

          break;
        }
        }
      });
}

enum class INDEX_OPTION : uint8_t {
  NONE = 0U,
  AVL = 1U << 0U,
  ISAM = 1U << 1U,
  SEQUENTIAL = 1U << 2U
};
static auto operator<<(std::ostream &ost, const INDEX_OPTION &option)
    -> auto & {

  switch (option) {

  case INDEX_OPTION::NONE:
    return ost << "NONE";
  case INDEX_OPTION::AVL:
    return ost << "AVL";
  case INDEX_OPTION::ISAM:
    return ost << "ISAM";
  case INDEX_OPTION::SEQUENTIAL:
    return ost << "SEQUENTIAL";
  }

  return ost << static_cast<uint8_t>(option);
}

constexpr auto operator+=(INDEX_OPTION &lhs, const INDEX_OPTION &rhs)
    -> INDEX_OPTION & {
  lhs = static_cast<INDEX_OPTION>(static_cast<uint8_t>(lhs) |
                                  static_cast<uint8_t>(rhs));
  return lhs;
}

struct IndexInsert {

  IndexInsert() = default;

  template <ValidType T>
  IndexInsert(const std::vector<T> &_values) : values(_values) {
    if (std::is_same_v<std::vector<int>, decltype(_values)>) {
    }
  }

  template <ValidType T, class... Args> void emplace_back(Args &&...args) {
    std::get<std::vector<T>>(values).emplace_back(std::forward<Args>(args)...);
  }
  template <ValidType T> void reserve(const auto &reserve_size) {
    std::get<std::vector<T>>(values).reserve(reserve_size);
  }
  template <ValidType T> [[nodiscard]] auto at(const auto &idx) const -> T {
    return std::get<std::vector<T>>(values).at(idx);
  }
  template <ValidType T> [[nodiscard]] auto size() const -> ulong {
    return std::get<std::vector<T>>(values).size();
  }

  std::variant<std::vector<int>, std::vector<float>, std::vector<std::string>>
      values;
};

template <ValidType T>
static void handle_option(
    const INDEX_OPTION &option, const IndexInsert &inserted_keys,
    const std::vector<bool> &pk_insertion,
    std::optional<std::reference_wrapper<AvlIndexContainer>> avl,
    std::optional<std::reference_wrapper<SequentialIndexContainer>> sequential,
    std::optional<std::reference_wrapper<IsamIndexContainer>> isam,
    const auto &get_pos) {

  spdlog::info("Inserting into {}", static_cast<uint8_t>(option));
  spdlog::info("values");
  spdlog::info("NONE {}", static_cast<uint8_t>(INDEX_OPTION::NONE));
  spdlog::info("avl {}", static_cast<uint8_t>(INDEX_OPTION::AVL));
  spdlog::info("isam {}", static_cast<uint8_t>(INDEX_OPTION::ISAM));
  spdlog::info("seq {}", static_cast<uint8_t>(INDEX_OPTION::SEQUENTIAL));

  std::bitset<3> options(static_cast<uint8_t>(option));

  std::vector<std::pair<T, std::streampos>> inserted_indexes;
  inserted_indexes.reserve(inserted_keys.size<T>());

  for (ulong i = 0; i < inserted_keys.size<T>(); i++) {
    if (pk_insertion.at(i)) {
      inserted_indexes.emplace_back(
          std::make_pair(inserted_keys.at<T>(i), get_pos()));
    }
  }

  if (options.test(0)) {
    spdlog::info("Found avl");
    std::jthread avl_insert([&avl, &inserted_indexes]() {
      avl.value().get().template bulk_insert<T>(inserted_indexes);
    });
  }
  if (options.test(1)) {
    spdlog::info("Found isam");
    std::jthread isam_insert([&isam, &inserted_indexes]() {
      isam.value().get().template bulk_insert<T>(inserted_indexes);
    });
  }
  if (options.test(2)) {
    spdlog::info("Found seq");
    std::jthread sequential_insert([&sequential, &inserted_indexes]() {
      sequential.value().get().bulk_insert(inserted_indexes);
    });
  }
}

static auto generate_key_output(const std::vector<DB_ENGINE::Type> &field_types,
                                const ulong &min_record_count)
    -> std::vector<IndexInsert> {

  std::vector<IndexInsert> inserted_keys;
  inserted_keys.resize(field_types.size());

  for (ulong idx = 0; const auto &idx_type : field_types) {
    switch (idx_type.type) {
    case DB_ENGINE::Type::INT: {

      IndexInsert insert_vec((std::vector<int>()));
      inserted_keys.at(idx) = insert_vec;
      inserted_keys.at(idx).reserve<int>(min_record_count);
      break;
    }
    case DB_ENGINE::Type::FLOAT: {
      IndexInsert insert_vec((std::vector<float>()));
      inserted_keys.at(idx) = insert_vec;
      inserted_keys.at(idx).reserve<float>(min_record_count);
      break;
    }
    case DB_ENGINE::Type::VARCHAR: {
      IndexInsert insert_vec((std::vector<std::string>()));
      inserted_keys.at(idx) = insert_vec;
      inserted_keys.at(idx).reserve<std::string>(min_record_count);
      break;
    }
    case DB_ENGINE::Type::BOOL:
      break;
    }
    idx++;
  }
  return inserted_keys;
}

void insert_field(const auto &field, const ulong &curr_field,
                  const std::vector<DB_ENGINE::Type> &field_types,
                  std::vector<IndexInsert> &inserted_keys) {

  switch (field_types.at(curr_field).type) {
  case DB_ENGINE::Type::INT: {
    auto field_val = std::string(field.begin(), field.end());

    inserted_keys.at(curr_field)
        .template emplace_back<int>(std::stoi(field_val));
    break;
  }

  case DB_ENGINE::Type::FLOAT: {

    inserted_keys.at(curr_field)
        .template emplace_back<float>(
            std::stof(std::string(field.begin(), field.end())));
    break;
  }
  case DB_ENGINE::Type::VARCHAR: {
    inserted_keys.at(curr_field)
        .template emplace_back<std::string>(field.begin(), field.end());
    break;
  }
  case DB_ENGINE::Type::BOOL:
    spdlog::info("Bool was not inserted");
    break;
  }
}

template <typename T>
auto get_idx(std::string table_name, std::string attribute_name, T idx_map)
    -> std::optional<std::reference_wrapper<typename T::mapped_type>> {
  if (idx_map.contains({table_name, attribute_name})) {
    return idx_map.at({table_name, attribute_name});
  }
  return std::nullopt;
}

void DBEngine::csv_insert(const std::string &table_name,
                          const std::filesystem::path &file) {

  // auto rec_size = m_tables_raw.at(table_name).get_record_size();
  // auto pos_getter = [&table_name, this, &rec_size]() {
  //   // Starting pos
  //   static std::streampos prev_pos =
  //       m_tables_raw.at(table_name).next_pos() -
  //       static_cast<std::streampos>(
  //           m_tables_raw.at(table_name).get_record_size());
  //   prev_pos += rec_size;
  //   return prev_pos;
  // };
  //
  // std::filesystem::path table_path = TABLES_PATH + table_name;
  // std::filesystem::path csv_path = CSV_PATH + file.filename().string() +
  // ".csv";
  //
  // if (!std::filesystem::exists(csv_path)) {
  //   throw std::runtime_error("File does not exist");
  // }
  //
  // auto &table_heap = m_tables_raw.at(table_name);
  //
  // auto [key_type, key_name] = table_heap.get_key_name();
  // auto primary_key_idx = table_heap.get_attribute_idx(key_name);
  //
  // std::ifstream csv_stream(csv_path);
  //
  // // Get csv size
  // csv_stream.seekg(0, std::ios::end);
  // auto csv_size = csv_stream.tellg();
  // auto min_record_count =
  //     static_cast<ulong>(csv_size / table_heap.get_record_size());
  // csv_stream.seekg(0, std::ios::beg);
  //
  // std::string current_line;
  //
  // // Field types
  // auto field_types = table_heap.get_types();
  // auto inserted_keys = generate_key_output(field_types, min_record_count);
  //
  // std::string key_value = get_sample_value(key_type);
  //
  // std::vector<bool> inserted_indexes;
  //
  // std::jthread pk_insertion;
  //
  // key_cast_and_execute(key_type.type, key_value, [&](auto sample) {
  //   // Iterate records
  //   using pk_type = decltype(sample);
  //
  //   if (std::is_same_v<int, pk_type>) {
  //     spdlog::info("inserting with key==int");
  //   }
  //
  //   std::vector<std::pair<pk_type, std::streampos>> pk_values;
  //
  //   if (std::is_same_v<std::vector<std::pair<int, std::streampos>>,
  //                      decltype(pk_values)>) {
  //     spdlog::info("inserting into pk_values ==vec int");
  //   }
  //
  //   auto pk_init_size = SequentialIndex<pk_type>::MIN_BULK_INSERT_SIZE::value
  //   /
  //                       sizeof(std::pair<pk_type, HeapFile::pos_type>);
  //   pk_values.reserve(min_record_count);
  //
  //   std::vector<Record> records;
  //   while (std::getline(csv_stream, current_line)) {
  //
  //     auto fields = current_line | std::ranges::views::split(',');
  //     records.emplace_back(fields);
  //
  //     // Iterate fields
  //     for (ulong curr_field = 0; auto field : fields) {
  //
  //       insert_field(field, curr_field, field_types, inserted_keys);
  //
  //       if (curr_field == primary_key_idx) {
  //
  //         key_value = std::string(field.begin(), field.end());
  //
  //         switch (key_type.type) {
  //
  //         case Type::BOOL: {
  //           spdlog::warn("Bool can't be pk");
  //           break;
  //         }
  //
  //         case Type::INT: {
  //           std::pair<int, std::streampos> i =
  //               std::make_pair(stoi(key_value), pos_getter());
  //           pk_values.push_back(i);
  //           break;
  //         }
  //
  //         case Type::FLOAT: {
  //           std::pair<float, std::streampos> i =
  //               std::make_pair(stof(key_value), pos_getter());
  //           pk_values.push_back(i);
  //           break;
  //         }
  //
  //           // case Type::VARCHAR: {
  //           //   std::pair<std::string, std::streampos> i =
  //           //       std::make_pair(key_value, pos_getter());
  //           //
  //           //   pk_values.push_back(i);
  //           //
  //           //   break;
  //           // }
  //         }
  //
  //         if (curr_field > pk_init_size) {
  //           pk_insertion = std::jthread([&inserted_indexes, &table_name,
  //                                        &key_name, &pk_values, this]() {
  //             spdlog::info("Bulk inserting pk - 1");
  //             auto bulk_insert_response =
  //                 m_sequential_indexes.at({table_name, key_name})
  //                     .bulk_insert<pk_type>(pk_values);
  //             inserted_indexes = bulk_insert_response.second;
  //           });
  //         }
  //       }
  //
  //       curr_field++;
  //     }
  //   }
  //
  //   table_heap.bulk_insert(records);
  //
  //   if (pk_insertion.joinable()) {
  //     pk_insertion.join();
  //   }
  //
  //   spdlog::info("Inserting into primary key index n° - rest {}",
  //                pk_values.size());
  //
  //   for (const auto &elem : pk_values) {
  //     std::cout << elem.first << ' ' << elem.second << '\n';
  //   }
  //
  //   auto inserted_bools = m_sequential_indexes.at({table_name, key_name})
  //                             .bulk_insert<pk_type>(pk_values)
  //                             .second;
  //   spdlog::info("Recieved bool count: {}", inserted_bools.size());
  //
  //   inserted_indexes.insert(inserted_indexes.end(), inserted_bools.begin(),
  //                           inserted_bools.end());
  // });
  //
  // // get non-primary indexes
  // std::vector<INDEX_OPTION> available_indexes;
  // available_indexes.resize(field_types.size(), INDEX_OPTION::NONE);
  //
  // // Fill available_indexes
  // auto process_indexes = [&table_name, &available_indexes, &table_heap](
  //                            const auto &indexes, INDEX_OPTION option) {
  //   auto count = 0;
  //   for (const auto &[idx, UNUSED] : indexes) {
  //     if (idx.table == table_name) {
  //       std::cout << "Found available index on :" << idx.attribute_name << '
  //       '
  //                 << count << ' ' << option << '\n';
  //       available_indexes.at(
  //           table_heap.get_attribute_idx(idx.attribute_name)) += option;
  //     }
  //     count++;
  //   }
  // };
  // process_indexes(m_sequential_indexes, INDEX_OPTION::SEQUENTIAL);
  // process_indexes(m_isam_indexes, INDEX_OPTION::ISAM);
  // process_indexes(m_avl_indexes, INDEX_OPTION::AVL);
  //
  // // Insert records into available indexes
  // auto attribute_names = table_heap.get_attribute_names();
  //
  // spdlog::info("Inserting into available indexes");
  // for (ulong idx_c = 0; const auto &idx : available_indexes) {
  //
  //   spdlog::info("Inserting into index n° {}", idx_c);
  //
  //   switch (field_types.at(idx_c).type) {
  //
  //   case Type::INT:
  //     handle_option<int>(
  //         idx, inserted_keys.at(idx_c), inserted_indexes,
  //         get_idx(table_name, attribute_names.at(idx_c), m_avl_indexes),
  //         get_idx(table_name, attribute_names.at(idx_c),
  //         m_sequential_indexes), get_idx(table_name,
  //         attribute_names.at(idx_c), m_isam_indexes), pos_getter);
  //     break;
  //   case Type::FLOAT:
  //     handle_option<float>(
  //         idx, inserted_keys.at(idx_c), inserted_indexes,
  //         get_idx(table_name, attribute_names.at(idx_c), m_avl_indexes),
  //         get_idx(table_name, attribute_names.at(idx_c),
  //         m_sequential_indexes), get_idx(table_name,
  //         attribute_names.at(idx_c), m_isam_indexes), pos_getter);
  //     break;
  //   case Type::VARCHAR:
  //     handle_option<std::string>(
  //         idx, inserted_keys.at(idx_c), inserted_indexes,
  //         get_idx(table_name, attribute_names.at(idx_c), m_avl_indexes),
  //         get_idx(table_name, attribute_names.at(idx_c),
  //         m_sequential_indexes), get_idx(table_name,
  //         attribute_names.at(idx_c), m_isam_indexes), pos_getter);
  //     break;
  //   case Type::BOOL:
  //     spdlog::error(
  //         "Bool can't be indexed. at: inserting into available indexes.");
  //     break;
  //   }
  //
  //   idx_c++;
  // }
}

auto DBEngine::csv_insert(const std::filesystem::path & /*file*/) -> bool {
  return {};
}

auto DBEngine::is_table(const std::string &table_name) const -> bool {
  return m_tables_raw.contains(table_name);
}

auto DBEngine::get_table_attributes(const std::string &table_name) const
    -> std::vector<std::string> {
  return m_tables_raw.at(table_name).get_attribute_names();
}

auto DBEngine::get_indexes(const std::string &table_name) const
    -> std::vector<Index_t> {
  return m_index_map.at(table_name);
}
auto DBEngine::get_indexes_names(const std::string &table_name) const
    -> std::vector<std::string> {
  std::vector<std::string> indexes_names;

  for (const auto &avl_index : m_avl_indexes) {
    if (avl_index.first.table == table_name) {
      indexes_names.push_back(avl_index.first.attribute_name);
    }
  }
  for (const auto &isam_index : m_isam_indexes) {
    if (isam_index.first.table == table_name) {
      indexes_names.push_back(isam_index.first.attribute_name);
    }
  }
  for (const auto &sequential_index : m_sequential_indexes) {
    if (sequential_index.first.table == table_name) {
      indexes_names.push_back(sequential_index.first.attribute_name);
    }
  }

  return indexes_names;
}

auto DBEngine::get_comparator(const std::string &table_name, Comp cmp,
                              const std::string &column_name,
                              const std::string &string_to_compare) const
    -> std::function<bool(const Record &record)> {

  auto type = m_tables_raw.at(table_name).get_type(column_name);
  auto index = m_tables_raw.at(table_name).get_attribute_idx(column_name);

  return [&type, &cmp, &string_to_compare, &index](const Record &record) {
    auto attribute_raw = record.m_fields.at(index);
    cast_and_execute(
        type.type, string_to_compare, attribute_raw,
        [&cmp](auto compare_value, auto record_value) {
          switch (cmp) {
          case EQUAL:
            if constexpr (std::is_same_v<decltype(compare_value), float>) {
              return compare_value - record_value < FLOAT_EPSILON;
            } else {
              return compare_value == record_value;
            }
          case LE:
            return compare_value <= record_value;
          case G:
            return compare_value > record_value;

          case L:
            return compare_value < record_value;
          case GE:
            return compare_value >= record_value;
          }
          throw std::runtime_error("Not valid comparator");
        });
    return cmp == EQUAL;
  };
}

void DBEngine::sort_attributes(const std::string &table_name,
                               std::vector<std::string> &attributes) const {
  // sort based on m_tables_raw.get_attribute_idx(string) value

  std::ranges::sort(attributes, [&](const auto &val1, const auto &val2) {
    return m_tables_raw.at(table_name).get_attribute_idx(val1) <
           m_tables_raw.at(table_name).get_attribute_idx(val2);
  });
}

void DBEngine::clean_table(const std::string &table_name) {
  std::filesystem::remove_all(TABLES_PATH + table_name);
}

void DBEngine::drop_table(const std::string &table_name) {
  std::filesystem::remove_all(TABLES_PATH + table_name);
  m_tables_raw.erase(table_name);

  for (auto &idx : m_avl_indexes) {
    if (idx.first.table == table_name) {
      m_avl_indexes.erase(idx.first);
    }
  }
  for (auto &idx : m_isam_indexes) {
    if (idx.first.table == table_name) {
      m_avl_indexes.erase(idx.first);
    }
  }
  for (auto &idx : m_sequential_indexes) {
    if (idx.first.table == table_name) {
      m_avl_indexes.erase(idx.first);
    }
  }
}

void DBEngine::generate_directories() {

  std::string base_path(BASE_PATH);

  bool created_base_dir = create_directory(base_path);
  spdlog::info(created_base_dir ? "Initialized filesystem"
                                : "Loaded filesystem");

  // Tables metadata
  if (create_directory(TABLES_PATH)) {
    spdlog::info("Created Tables subdir");
  }

  // Metadata
  if (create_directory(METADATA_PATH)) {
    spdlog::info("Created Metadata subdir");
  }

  // Indexes
  if (create_directory(INDEX_PATH)) {
    spdlog::info("Created Indexes subdir");
  }

  // Create subdirs for each index type
  for (const auto &idx_name : Constants::INDEX_TYPES) {

    if (create_directory(INDEX_PATH "/" + idx_name)) {
      spdlog::info("Created {} subdir", idx_name);
    }
  }

  spdlog ::info("DBEngine created");
}
