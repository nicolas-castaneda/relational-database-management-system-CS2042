#ifndef INDEX_CONCEPTS_HPP
#define INDEX_CONCEPTS_HPP

#include "response.hpp"
#include <concepts>

template <typename T>
// T must be int, float, string or bool
concept ValidType = std::same_as<T, int> || std::same_as<T, float> ||
                    std::same_as<T, std::string> || std::same_as<T, bool>;

template <typename T>
concept ValidIndexType = std::same_as<T, int> || std::same_as<T, float> ||
                         std::same_as<T, std::string>;

template <typename T> auto str_cast(const T &value) -> std::string {
  return std::to_string(value);
}
template <> inline auto str_cast(const std::string &value) -> std::string {
  return value;
}

using response_time = std::chrono::milliseconds;

template <typename U, template <typename> class T>
concept ValidIndexHelper = ValidIndexType<U> && requires(T<U> idx, U u) {
  { idx.get_attribute_name() } -> std::same_as<std::string>;
  { idx.get_table_name() } -> std::same_as<std::string>;
  { idx.range_search(u, u) } -> std::same_as<Index::Response>;
  { idx.remove(u) } -> std::same_as<Index::Response>;
  { idx.search(u) } -> std::same_as<Index::Response>;
  {
    idx.add(u, std::declval<std::streampos>())
  } -> std::same_as<Index::Response>;
  {
    idx.bulk_insert(
        std::declval<const std::vector<std::pair<U, std::streampos>> &>())
  } -> std::same_as<std::pair<Index::Response, std::vector<bool>>>;

  { T<U>::MIN_BULK_INSERT_SIZE::value } -> std::same_as<const size_t &>;
};

template <template <typename> class T>
concept ValidIndex = ValidIndexHelper<int, T> && ValidIndexHelper<float, T> &&
                     ValidIndexHelper<std::string, T>;

#endif // !INDEX_CONCEPTS_HPP
