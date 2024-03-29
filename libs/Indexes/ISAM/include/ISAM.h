//
// Created by renat on 9/7/2023.
//

#ifndef INDEXED_SEQUENTIAL_ACCESS_METHOD_ISAM_H
#define INDEXED_SEQUENTIAL_ACCESS_METHOD_ISAM_H

#include "../../index.hpp"
#include "../../response.hpp"
#include "ISAMRecord.h"
#include "Metadata.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <vector>

constexpr int64_t ISAM_LEVELS = 3;

/*
 * Heuristic ISAM
 *
 * About ISAM:
 *      This implementation has 3 levels and
 *      manage the collision with chaining.
 *
 */

template <typename KeyType> class ISAM {
private:
  mutable Metadata<KeyType> metadata;
  mutable POS_TYPE root; // Root of the ISAM

  std::string idx_filename =
      "isam_index_file.dat";                      // Name of the index page file
  std::string dt_filename = "isam_data_file.dat"; // Name of the data page file

  mutable std::fstream idx_file; // Index page file
  mutable std::fstream dt_file;  // Data page file
  mutable std::fstream dup_file; // Data page file

  std::string directory = "./DB_FILES/Indexes";
  std::string attribute_name;
  std::string table_name;
  std::string index_name;
  std::string duplicatesFilename = "duplicate_isam_data_file.dat";
  bool PK;

public:
  ISAM() = default;
  using MIN_BULK_INSERT_SIZE = std::integral_constant<size_t, 81>;

  ISAM(std::string _table_name, std::string _attribute_name, bool _pK = false);
  // ISAM(ISAM &&other) noexcept = default;
  // ISAM(const ISAM &other) = default;

  std::string get_attribute_name() const;

  std::string get_table_name() const;

  std::string get_index_name() const { return this->index_name; };

  const std::string &getIdxFilename() const;

  const std::string &getDtFilename() const;

  //    ISAM(std::vector<ISAMRecord<KeyType>> &data);

  bool open_files(std::ios::openmode op) const;

  void close_files() const;

  POS_TYPE max_records() const { return MIN_BULK_INSERT_SIZE::value; }

  Index::Response search(Data<KeyType> key) const;

  Index::Response __add(ISAMRecord<KeyType> record) const;

  Index::Response remove(Data<KeyType> key) const;

  Index::Response range_search(Data<KeyType> beg, Data<KeyType> end) const;

  Index::Response add(Data<KeyType> key, POS_TYPE raw_pos) const {
    return __add(ISAMRecord<KeyType>{key, raw_pos});
  }

  // ~ISAM();

  void build(std::vector<ISAMRecord<KeyType>> &data) const;

  std::pair<Index::Response, std::vector<bool>>
  bulk_insert(const std::vector<std::pair<KeyType, POS_TYPE>> &data) const {
    Index::Response response;
    response.startTimer();
    std::vector<ISAMRecord<KeyType>> other;
    for (auto &pair : data)
      other.push_back(ISAMRecord<KeyType>{pair.first, pair.second});
    build(other);
    response.stopTimer();
    response.records.emplace_back(1);

    return std::make_pair(response, std::vector{true});
  }

private:
  void create_files_if_not_exist();

  void _search(POS_TYPE node_pos, KeyType key,
               std::vector<POS_TYPE> &result) const;

  void _insert(POS_TYPE node_pos, ISAMRecord<KeyType> record,
               std::vector<POS_TYPE> &result) const;

  void _range_search(POS_TYPE node_pos, KeyType beg, KeyType end,
                     std::vector<POS_TYPE> &result) const;

  void _remove(POS_TYPE node_pos, KeyType key,
               std::vector<POS_TYPE> &result) const;

  void insertDuplicate(ISAMRecord<KeyType> &rec,
                       ISAMRecord<KeyType> &rec_dup) const;

  void insertDuplicateFile(ISAMRecord<KeyType> &rec,
                           POS_TYPE &duplicate_position) const;

  void getAllRawCurrentRecords(ISAMRecord<KeyType> record,
                               std::vector<POS_TYPE> &records) const;

  void splitAndInsert(DataPage<KeyType> &page,
                      ISAMRecord<KeyType> record) const;

  void merge(DataPage<KeyType> &page) const;
};

extern template class ISAM<int>;
extern template class ISAM<float>;

#endif // INDEXED_SEQUENTIAL_ACCESS_METHOD_ISAM_H
