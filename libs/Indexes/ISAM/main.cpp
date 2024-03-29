#include "include/ISAM.h"
#include <unordered_map>
#include <variant>

template <typename T>
concept ValidType = true;

class Response {};

using response_time = std::chrono::nanoseconds;

template <template <typename> class IndexType> struct IndexContainer {

  template <ValidType T>
  explicit IndexContainer(IndexType<T> &idx) : m_idx{std::move(idx)} {}

  template <ValidType T>
  [[nodiscard]] auto range_search(T begin, T end) const -> Index::Response {
    return std::get<IndexType<T>>(m_idx).range_search(begin, end);
  }

  [[nodiscard]] auto get_attribute_name() const -> std::string {
    return std::visit(
        [](const auto &index) { return index.get_attribute_name(); }, m_idx);
  }
  [[nodiscard]] auto get_table_name() const -> std::string {
    return std::visit([](const auto &index) { return index.get_table_name(); },
                      m_idx);
  }

  template <ValidType T>
  [[nodiscard]] auto remove(T key) const
      -> std::pair<std::streampos, response_time> {
    auto idx_response = std::get<IndexType<T>>(m_idx).remove(key);
    return {idx_response.records.at(0), idx_response.query_time};
  }

  template <ValidType T>
  [[nodiscard]] auto search(T key) const
      -> std::pair<std::vector<std::streampos>, response_time> {
    auto idx_response = std::get<IndexType<T>>(m_idx).search(key);
    return {idx_response.records, idx_response.query_time};
  }

  template <ValidType T>
  [[nodiscard]] auto add(T key, std::streampos pos)
      -> std::pair<bool, response_time> {
    auto idx_response = std::get<IndexType<T>>(m_idx).add(key, pos);
    return {!idx_response.records.empty(), idx_response.query_time};
  }

  template <ValidType T>
  auto bulk_insert(std::vector<std::pair<T, std::streampos>> &elements)
      -> std::pair<Response, std::vector<bool>> {
    return std::get<IndexType<T>>(m_idx).bulk_insert(elements);
  }

  std::variant<IndexType<int>, IndexType<float> /* , IndexType<std::string> */>
      m_idx;
};

struct IsamIndexContainer : public IndexContainer<ISAM> {
  template <typename T>
  IsamIndexContainer(ISAM<T> &idx) : IndexContainer<ISAM>{idx} {}
};

namespace DB_ENGINE {
struct Index {

  std::string table;
  std::string attribute_name;

  // spaceship operator
  friend auto operator<=>(const Index &, const Index &) = default;
};
}; // namespace DB_ENGINE

struct HashPair {
  auto operator()(const DB_ENGINE::Index &index) const -> size_t {
    auto hash1 = std::hash<std::string>{}(index.table);
    auto hash2 = std::hash<std::string>{}(index.attribute_name);
    if (hash1 != hash2) {
      return hash1 ^ hash2;
    }
    // If hash1 == hash2, their XOR is zero.
    return hash1;
  }
};
int main() {

  std::unordered_map<DB_ENGINE::Index, IsamIndexContainer, HashPair> indexes;

  ISAM<int> idx("aef", "feiojwf", false);
  indexes.emplace(DB_ENGINE::Index("efjo", "sfeoi"), idx);
  auto x = indexes.at({"efjo", "sfeoi"}).get_attribute_name();
}
