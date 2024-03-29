#ifndef INDEX_FILE_HPP
#define INDEX_FILE_HPP

#include "data.hpp"
#include "response.hpp"
#include "utils.hpp"

namespace Index {
template <typename KEY_TYPE> class Index {
protected:
  std::string directory = "./DB_FILES/Indexes";
  std::string attribute_name;
  std::string table_name;
  std::string index_name;
  std::string duplicatesFilename;
  bool PK;

  template <typename RecordType>
  void getAllRawCurrentRecords(RecordType sir,
                               std::vector<physical_pos> &records) const;

  template <typename FileType = std::fstream, typename RecordType>
  void insertDuplicate(FileType &file, RecordType &sir,
                       RecordType &sir_dup) const;

  template <typename RecordType>
  void insertDuplicateFile(RecordType &sir,
                           physical_pos &duplicate_position) const;

  template <typename HeaderType, typename RecordType>
  size_t numberRecordsWithHeader(std::string file_name) const;

  template <typename HeaderType, typename RecordType>
  void printFileWithHeader(std::string file_name) const;

  template <typename RecordType>
  size_t numberRecords(std::string file_name) const;

  template <typename RecordType>
  void printFile(const std::string &file_name) const;

  template <typename FileType = std::fstream, typename HeaderType>
  void writeHeader(FileType &file, HeaderType &header) const;

  template <typename FileType = std::fstream, typename HeaderType>
  void readHeader(FileType &file, HeaderType &header) const;

  template <typename FileType = std::fstream, typename RecordType>
  void readRecord(FileType &file, RecordType &record) const;

  template <typename FileType = std::fstream, typename RecordType>
  void writeRecord(FileType &file, RecordType &record) const;

  template <typename FileType = std::fstream, typename RecordType>
  void moveReadRecord(FileType &file, physical_pos &pos,
                      RecordType &record) const;

  template <typename FileType = std::fstream, typename RecordType>
  void moveWriteRecord(FileType &file, physical_pos &pos,
                       RecordType &record) const;

public:
  virtual ~Index() = default;

  std::string get_attribute_name() const { return this->attribute_name; }
  std::string get_table_name() const { return this->table_name; }

  virtual std::string get_index_name() const = 0;

  virtual Response add(Data<KEY_TYPE> data, physical_pos raw_pos) const = 0;
  virtual Response search(Data<KEY_TYPE> data) const = 0;
  virtual Response range_search(Data<KEY_TYPE> begin,
                                Data<KEY_TYPE> end) const = 0;
  virtual Response remove(Data<KEY_TYPE> data) const = 0;
  virtual std::pair<Response, std::vector<bool>>
  bulk_insert(const std::vector<std::pair<Data<KEY_TYPE>, physical_pos>> &data)
      const = 0;
};

#include "index.tpp"

} // namespace Index

#endif // INDEX_FILE_HPP
