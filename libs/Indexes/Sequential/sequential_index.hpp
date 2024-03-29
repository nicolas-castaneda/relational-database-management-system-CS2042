#ifndef SEQUENTIAL_INDEX_HPP
#define SEQUENTIAL_INDEX_HPP

#include "binary_search_response.hpp"
#include "index.hpp"
#include "sequential_index_header.hpp"
#include "sequential_index_record.hpp"
#include <filesystem>
#include <stdexcept>
#include <type_traits>

template <typename KEY_TYPE = default_data_type>
class SequentialIndex : public Index::Index<KEY_TYPE> {
  std::string indexFilename;
  std::string auxFilename;
  /*
      Helper functions
  */
  void createFile();
  bool fileExists();

  bool validNumberRecords() const;

  void searchAuxFile(Data<KEY_TYPE> data, BinarySearchResponse<KEY_TYPE> &bir,
                     std::vector<physical_pos> &records,
                     SequentialIndexRecord<KEY_TYPE> &sir) const;

  template <typename FileType = std::fstream>
  void insertAux(FileType &indexFile, SequentialIndexRecord<KEY_TYPE> &sir_init,
                 SequentialIndexRecord<KEY_TYPE> &sir,
                 BinarySearchResponse<KEY_TYPE> &bsr) const;

  void insertAuxFile(SequentialIndexRecord<KEY_TYPE> &sir) const;

  template <typename FileType = std::fstream>
  void insertAfterRecord(FileType &file,
                         SequentialIndexRecord<KEY_TYPE> &sir_prev,
                         SequentialIndexRecord<KEY_TYPE> &sir,
                         SequentialIndexHeader &sih, bool header) const;

  Index::Response add(Data<KEY_TYPE> data, physical_pos raw_pos,
                      bool rebuild) const;
  Index::Response erase(Data<KEY_TYPE> data, Index::Response &response) const;

  /*
      Binary search in files
  */
  template <typename FileType = std::fstream>
  BinarySearchResponse<KEY_TYPE> binarySearch(FileType &file,
                                              Data<KEY_TYPE> data) const;

public:
  using MIN_BULK_INSERT_SIZE = std::integral_constant<size_t, 0>;

  /*
      Constructor
  */
  SequentialIndex(std::string _table_name, std::string _attribute_name,
                  bool PK = false) {
    this->table_name = _table_name;
    this->attribute_name = _attribute_name;
    this->index_name = "Sequential";

    // DB_FILES/Sequential/<table_name>_<attribute_name>/<table_name>_<attribute_name>_<index_name>_indexFile.bin

    std::string _directory = this->directory + "/" + this->index_name + "/" +
                             this->table_name + "/" + this->attribute_name;

    this->indexFilename = _directory + "/" + _table_name + "_" +
                          _attribute_name + "_" + this->index_name +
                          "_indexFile.bin";
    this->auxFilename = _directory + "/" + _table_name + "_" + _attribute_name +
                        "_" + this->index_name + "_auxFile.bin";
    this->duplicatesFilename = _directory + "/" + _table_name + "_" +
                               _attribute_name + "_" + this->index_name +
                               "_duplicateFile.bin";
    this->PK = PK;

    if (!std::filesystem::is_directory(_directory)) {
      std::filesystem::create_directories(_directory);
      createFile();
    }
  }

  std::string get_index_name() const override;

  /*
      Query functions
  */

  Index::Response add(Data<KEY_TYPE> data, physical_pos raw_pos) const override;
  Index::Response search(Data<KEY_TYPE> data) const override;
  Index::Response range_search(Data<KEY_TYPE> begin,
                               Data<KEY_TYPE> end) const override;
  Index::Response remove(Data<KEY_TYPE> data) const override;

  Index::Response addNotRebuild(Data<KEY_TYPE> data, physical_pos raw_pos);

  Index::Response
  loadRecords(std::vector<std::pair<Data<KEY_TYPE>, physical_pos>> &records);

  void rebuild() const;

  std::pair<Index::Response, std::vector<bool>> bulk_insert(
      const std::vector<std::pair<Data<KEY_TYPE>, physical_pos>> &records)
      const override;

  /*
      Print files sequentially
  */
  void printIndexFile() const;
  void printAuxFile() const;
  void printDuplicatesFile() const;
};

#include "sequential_index.tpp"

#endif // SEQUENTIAL_INDEX_HPP
