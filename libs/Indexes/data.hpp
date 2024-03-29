#ifndef DATA_HPP
#define DATA_HPP

#include <concepts>
#include <ostream>

template <typename T>
concept ValidData = std::equality_comparable<T> &&
                    std::totally_ordered<T> && // Printable to std::ostream
                    requires(T a, std::ostream &os) {
                      { os << a } -> std::same_as<std::ostream &>;
                    }

;

template <ValidData T> using Data = T;

// template <typename KEY_TYPE> struct Data {
//   KEY_TYPE key;
//
//   Data() = default;
//
//   Data(KEY_TYPE _key) { this->key = _key; }
//
//   friend std::ostream &operator<<(std::ostream &stream,
//                                   const Data<KEY_TYPE> &data) {
//     stream << " | key: " << data.key;
//     return stream;
//   }
//
//   bool operator==(const Data<KEY_TYPE> &other) const {
//     return this->key == other.key;
//   }
//
//   bool operator<(const Data<KEY_TYPE> &other) const {
//     return this->key < other.key;
//   }
//
//   bool operator<=(const Data<KEY_TYPE> &other) const {
//     return this->key <= other.key;
//   }
//
//   bool operator>(const Data<KEY_TYPE> &other) const {
//     return this->key > other.key;
//   }
//
//   bool operator>=(const Data<KEY_TYPE> &other) const {
//     return this->key >= other.key;
//   }
// };

#endif // DATA_HPP
