#ifndef INDEX_CONTAINER_HPP
#define INDEX_CONTAINER_HPP

#include "IndexConcept.hpp"
#include <spdlog/spdlog.h>

#include "AVL/avl_index.hpp"
#include "ISAM/include/ISAM.h"
#include "Sequential/sequential_index.hpp"

template <template <typename> class IndexType>
  requires ValidIndex<IndexType>
struct IndexContainer {

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
    spdlog::info("calling bulk_insert on idx");
    return std::get<IndexType<T>>(m_idx).bulk_insert(elements);
  }

  std::variant<IndexType<int>, IndexType<float>, IndexType<std::string>> m_idx;
};

struct SequentialIndexContainer : public IndexContainer<SequentialIndex> {
  template <ValidIndexType T>
  SequentialIndexContainer(SequentialIndex<T> &idx)
      : IndexContainer<SequentialIndex>{idx} {}
};

struct AvlIndexContainer : public IndexContainer<AVLIndex> {
  template <ValidIndexType T>
  AvlIndexContainer(AVLIndex<T> &idx) : IndexContainer<AVLIndex>{idx} {}
};

struct IsamIndexContainer : public IndexContainer<ISAM> {
  template <ValidIndexType T>
  IsamIndexContainer(ISAM<T> &idx) : IndexContainer<ISAM>{idx} {}
};

#endif // !AVL_INDEX_CONTAINER_HPP
