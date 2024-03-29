//
// Created by renat on 9/7/2023.
//

#include "../include/ISAM.h"

using Index::Response;

template <typename KeyType>
ISAM<KeyType>::ISAM(std::string _table_name, std::string _attribute_name,
                    bool _pK) {
  this->table_name = _table_name;
  this->attribute_name = _attribute_name;
  this->index_name = "IsamIndex";
  this->PK = _pK;

  std::string _directory = this->directory + "/" + this->index_name + "/" +
                           this->table_name + "/" + this->attribute_name;

  this->idx_filename = _directory + "/" + _table_name + "_" + _attribute_name +
                       "_" + this->index_name + "_indexPages.bin";

  this->dt_filename = _directory + "/" + _table_name + "_" + _attribute_name +
                      "_" + this->index_name + "_dataPages.bin";

  this->duplicatesFilename = _directory + "/" + _table_name + "_" +
                             _attribute_name + "_" + this->index_name +
                             "_duplicateFile.bin";

  if (!std::filesystem::is_directory(_directory)) {
    std::filesystem::create_directories(_directory);
    create_files_if_not_exist();
  }

  root = -1;

  close_files();
};

// template<typename KeyType>
// ISAM<KeyType>::ISAM(std::vector<ISAMRecord<KeyType>> &data) {
//
//     root = metadata.getRootPosition();
//
//     create_files_if_not_exist();
//     close_files();
//
//     build(data);
// }

template <typename KeyType>
bool ISAM<KeyType>::open_files(std::ios::openmode op) const {
  idx_file.open(idx_filename, op);
  dt_file.open(dt_filename, op);
  dup_file.open(this->duplicatesFilename, op);
  return (idx_file.good() && dt_file.good());
}

template <typename KeyType> void ISAM<KeyType>::close_files() const {
  idx_file.close();
  dt_file.close();
  dup_file.close();
}

template <typename KeyType>
const std::string &ISAM<KeyType>::getIdxFilename() const {
  return idx_filename;
}

template <typename KeyType>
const std::string &ISAM<KeyType>::getDtFilename() const {
  return dt_filename;
}

template <typename KeyType> void ISAM<KeyType>::create_files_if_not_exist() {
  // Create the index and data page file if not exist
  if (!open_files(std::ios::in)) {
    std::cout << "Files didn't exists" << std::endl;
    if (!open_files(std::ios::out)) {
      throw std::runtime_error("Files couldn't open");
    }
  }
}

template <typename KeyType>
void ISAM<KeyType>::build(std::vector<ISAMRecord<KeyType>> &data) const {
  POS_TYPE M = metadata.getIndexPageCapacity();
  POS_TYPE N = metadata.getDataPageCapacity();

  if (data.size() < static_cast<size_t>(max_records()))
    throw std::runtime_error("It's necesary have more than " +
                             std::to_string(max_records()) +
                             " in order to create the ISAM");

  auto compare = [](ISAMRecord<KeyType> &a, ISAMRecord<KeyType> &b) {
    return a.key < b.key;
  };
  std::sort(begin(data), end(data), compare);

  open_files(std::ios::binary | std::ios::out);

  POS_TYPE index = 0;       // Data index
  IndexPage<KeyType> page0; //  first level page
  for (int j = 0; j < M + static_cast<POS_TYPE>(1); ++j) {
    IndexPage<KeyType> page1; // second level page
    for (int k = 0; k < M + static_cast<POS_TYPE>(1); ++k) {
      IndexPage<KeyType> page2; // third level page
      for (int h = 0; h < M + static_cast<POS_TYPE>(1); ++h) {
        DataPage<KeyType> page3; // data level page
        for (int l = 0; l < N; ++l) {
          page3.setRecord(data[static_cast<unsigned long>(index)], l);
          index = index + static_cast<POS_TYPE>(1);
        }
        POS_TYPE pos = page3.write(dt_file);
        page2.setChild(pos, h);
        if (h < M)
          page2.setKey(data[static_cast<unsigned long>(index)].key, h);
      }
      page2.setIsLeaf(true);
      POS_TYPE pos = page2.write(idx_file);
      page1.setChild(pos, k);
      if (k < M)
        page1.setKey(data[static_cast<unsigned long>(index)].key, k);
    }
    page1.setIsLeaf(false);
    POS_TYPE pos = page1.write(idx_file);
    page0.setChild(pos, j);
    if (j < M)
      page0.setKey(data[static_cast<unsigned long>(index)].key, j);
  }
  page0.setIsLeaf(false);
  root = page0.write(idx_file);

  // Save root position in metadata
  metadata.setRootPosition(root);

  close_files();

  // Insert the rest of records
  if (data.size() > static_cast<size_t>(max_records())) {
    while (static_cast<size_t>(index) < data.size()) {
      this->__add(data[static_cast<unsigned long>(index)]);
      index = index + static_cast<POS_TYPE>(1);
    }
  }
}

template <typename KeyType>
Response ISAM<KeyType>::search(Data<KeyType> key) const {
  if (root == -1)
    throw std::runtime_error("No indexed data in ISAM, please build before");

  open_files(std::ios::binary | std::ios::in | std::ios::out);
  Response res;
  res.startTimer();
  _search(root, key, res.records);
  res.stopTimer();
  close_files();
  return res;
}

template <typename KeyType>
void ISAM<KeyType>::_search(POS_TYPE node_pos, KeyType key,
                            std::vector<POS_TYPE> &result) const {
  // Read the node
  idx_file.seekg(node_pos, std::ios::beg);
  IndexPage<KeyType> node;
  node.read(idx_file);

  POS_TYPE M = metadata.getIndexPageCapacity();

  int index = 0;
  // Search left-to-right where go down

  while (index < M && node.getkey(index) <= key) {
    ++index;
  }

  if (node.getIsLeaf()) {
    DataPage<KeyType> page;
    POS_TYPE data_pos = node.getChildren()[index];

    do {
      // Read the Data page
      dt_file.seekg(data_pos, std::ios::beg);
      page.read(dt_file);

      for (int i = 0; i < page.getCount(); ++i) {
        if (page.getKey(i) == key) {
          getAllRawCurrentRecords(page.getRecord(i), result);
          return;
        }
      }

      // Continue with the next page
      data_pos = page.getNext();

    } while (data_pos != -1);

  } else {
    // Call recursively search with the new node
    POS_TYPE new_pos = node.getChild(index);
    _search(new_pos, key, result);
  }
}

template <typename KeyType>
Response ISAM<KeyType>::__add(ISAMRecord<KeyType> record) const {
  if (root == -1)
    throw std::runtime_error("No indexed data in ISAM, please build before");

  open_files(std::ios::binary | std::ios::in | std::ios::out);
  Response res;
  res.startTimer();
  _insert(root, record, res.records);
  res.stopTimer();
  close_files();

  return res;
}

template <typename KeyType>
void ISAM<KeyType>::_insert(POS_TYPE node_pos, ISAMRecord<KeyType> record,
                            std::vector<POS_TYPE> &result) const {
  // Read the node
  idx_file.seekg(node_pos, std::ios::beg);
  IndexPage<KeyType> node;
  node.read(idx_file);

  POS_TYPE M = metadata.getIndexPageCapacity();

  int index = 0;
  // Find a key to descend through the tree
  KeyType *keys = node.getKeys();
  while (index < M && keys[index] <= record.key) {
    ++index;
  }

  if (node.getIsLeaf()) {

    DataPage<KeyType> page;
    POS_TYPE data_pos = node.getChildren()[index];
    POS_TYPE prev_pos = data_pos;

    do {
      // Read the Data page
      prev_pos = data_pos;
      dt_file.seekg(data_pos, std::ios::beg);
      page.read(dt_file);

      if (page.isEmpty()) {
        goto CREATE_PAGE;
      }

      if (record.key <= page.last().key) {
        // Check for duplicates
        int i = 0;
        for (; i < page.getCount(); ++i) {
          if (page.getKey(i) == record.key) {
            insertDuplicate(page.getRecord(i), record);
            dt_file.seekp(data_pos, std::ios::beg);
            page.write(dt_file);
            result.push_back(record.pos);
            return;
          }
        }

        if (page.isFull()) {
          // Split in two pages
          splitAndInsert(page, record);
          dt_file.seekp(data_pos, std::ios::beg);
          page.write(dt_file);
          result.push_back(record.pos);
          return;
        } else {
          page.insertRecord(record);
          dt_file.seekp(data_pos, std::ios::beg);
          page.write(dt_file);
          result.push_back(record.pos);
          return;
        }
      }

      // Continue with the next page
      data_pos = page.getNext();

    } while (data_pos != -1);

    node.getChild(index);
    // Insert in a non full sorted data page
    if (!page.isFull()) {
      page.insertRecord(record);
      dt_file.seekp(prev_pos, std::ios::beg);
      page.write(dt_file);
      result.push_back(record.pos);
      return;
    } else {
    // Create a new data page at the end of the file
    CREATE_PAGE:
      dt_file.seekp(0, std::ios::end);
      POS_TYPE new_pos = dt_file.tellp();

      DataPage<KeyType> new_page;
      new_page.setRecord(record, 0);
      new_page.write(dt_file);

      // Linking with the other page
      page.setNext(new_pos);
      dt_file.seekp(prev_pos, std::ios::beg);
      page.write(dt_file);

      result.push_back(record.pos);
      return;
    }
  } else {
    POS_TYPE pos = node.getChildren()[index];
    _insert(pos, record, result);
  }
}

template <typename KeyType>
void ISAM<KeyType>::splitAndInsert(DataPage<KeyType> &page,
                                   ISAMRecord<KeyType> record) const {
  POS_TYPE index =
      page.getCount() -
      static_cast<POS_TYPE>(1); // Position where the record should be inserted
  while (index > 0 && record.key < page.getKey(index)) {
    index = index - static_cast<POS_TYPE>(1);
  }

  DataPage<KeyType> new_page;
  new_page.setNext(page.getNext());

  POS_TYPE N = DataPage<KeyType>::getCapacity();
  int j = 0; // Position in the new page
  for (int i = static_cast<int>(std::floor(N / 2)); i < N; ++i) {
    new_page.setRecord(page.getRecord(i), j++);
    page.clearRecord(i);
  }

  if (index < static_cast<int>(std::floor(N / 2))) {
    page.insertRecord(record);
  } else {
    new_page.insertRecord(record);
  }

  dt_file.seekp(0, std::ios::end);
  POS_TYPE pos = new_page.write(dt_file);
  page.setNext(pos);
}

template <typename KeyType>
Response ISAM<KeyType>::range_search(Data<KeyType> beg,
                                     Data<KeyType> end) const {
  if (root == -1)
    throw std::runtime_error("No indexed data in ISAM, please build before");

  open_files(std::ios::binary | std::ios::in);
  Response res;
  res.startTimer();
  _range_search(root, beg, end, res.records);
  res.stopTimer();
  close_files();

  return res;
}

template <typename KeyType>
void ISAM<KeyType>::_range_search(POS_TYPE node_pos, KeyType beg, KeyType end,
                                  std::vector<POS_TYPE> &result) const {
  // Read the node
  idx_file.seekg(node_pos, std::ios::beg);
  IndexPage<KeyType> node;
  node.read(idx_file);

  POS_TYPE M = metadata.getIndexPageCapacity();

  // Find a key to descend through the tree
  int index = 0;
  KeyType *keys = node.getKeys();
  while (index < M && keys[index] < beg) {
    ++index;
  }

  if (node.getIsLeaf()) {
    // ISAM has (M+1)^level of DATA PAGES and each of them have a fix
    auto last_pos = static_cast<POS_TYPE>(std::ceil(
        pow(static_cast<double>(M + static_cast<POS_TYPE>(1)), ISAM_LEVELS) *
        static_cast<double>(DataPage<KeyType>::size_of())));

    KeyType key = beg;
    POS_TYPE data_pos = node.getChildren()[index];
    do {
      DataPage<KeyType> page;

      // Iterate through linked data pages
      POS_TYPE _pos = data_pos;
      do {
        // Read the Data page
        dt_file.seekg(_pos, std::ios::beg);
        page.read(dt_file);

        if (page.isEmpty())
          break;

        // Push the key in the range [beg, end]
        for (int i = 0; i < page.getCount(); ++i) {
          key = page.getKey(i);
          if (beg <= key && key <= end) {
            getAllRawCurrentRecords(page.getRecord(i), result);
          }
        }

        // Continue with the next page
        _pos = page.getNext();
      } while (_pos != -1);

      // Go to the next brother
      data_pos += page.size_of();
    } while (data_pos < last_pos && key <= end);
    return;
  } else {
    POS_TYPE new_pos = node.getChildren()[index];
    _range_search(new_pos, beg, end, result);
  }
}

template <typename KeyType>
Response ISAM<KeyType>::remove(Data<KeyType> key) const {
  if (root == -1)
    throw std::runtime_error("No indexed data in ISAM, please build before");

  open_files(std::ios::binary | std::ios::in | std::ios::out);
  Response res;
  res.startTimer();
  _remove(root, key, res.records);
  res.stopTimer();
  close_files();

  return res;
}

template <typename KeyType>
void ISAM<KeyType>::_remove(POS_TYPE node_pos, KeyType key,
                            std::vector<POS_TYPE> &result) const {
  // Read the node
  idx_file.seekg(node_pos, std::ios::beg);
  IndexPage<KeyType> node;
  node.read(idx_file);

  POS_TYPE M = metadata.getIndexPageCapacity();

  int index = 0;
  // Search left-to-right where go down
  KeyType *keys = node.getKeys();
  while (index < M && keys[index] <= key) {
    ++index;
  }

  if (node.getIsLeaf()) {
    DataPage<KeyType> page, prev_page;
    POS_TYPE deleted_pos = -1;
    POS_TYPE data_pos = node.getChildren()[index];
    POS_TYPE prev_pos = data_pos;
    int iter = 0;

    do {
      // Read the current page
      dt_file.seekg(data_pos, std::ios::beg);
      page.read(dt_file);
      // Read the previous page
      dt_file.seekg(prev_pos, std::ios::beg);
      prev_page.read(dt_file);

      if (page.isEmpty()) {
        return;
      }

      if (key <= page.last().key) {
        for (int i = 0; i < page.getCount(); ++i) {
          if (page.getKey(i) == key) {
            deleted_pos = page.getRecord(i).pos;
            page.remove(i);
          }
        }

        // Record not found
        if (deleted_pos == -1) {
          return;
        }

        if (page.isEmpty()) {
          if (iter == 0) {
            // Write current data page
            dt_file.seekp(data_pos, std::ios::beg);
            page.write(dt_file);

            result.push_back(deleted_pos);
            return;
          } else {
            prev_page.setNext(page.getNext());

            // Write previous data page
            dt_file.seekp(prev_pos, std::ios::beg);
            prev_page.write(dt_file);

            result.push_back(deleted_pos);
            return;
          }
        } else if (page.getCount() <=
                   static_cast<POS_TYPE>(
                       std::floor(DataPage<KeyType>::getCapacity() / 2))) {
          merge(page);

          // Write current data page
          dt_file.seekp(data_pos, std::ios::beg);
          page.write(dt_file);

          result.push_back(deleted_pos);
          return;
        } else {
          // Write current data page
          dt_file.seekp(data_pos, std::ios::beg);
          page.write(dt_file);

          result.push_back(deleted_pos);
          return;
        }
      }
      // Continue with the next page
      data_pos = page.getNext();
      ++iter;
    } while (data_pos != -1);

  } else {
    // Call recursively search with the new node
    POS_TYPE new_pos = node.getChildren()[index];
    return _remove(new_pos, key, result);
  }
}

template <typename KeyType>
void ISAM<KeyType>::merge(DataPage<KeyType> &page) const {
  if (page.getNext() == -1) {
    return;
  }
  DataPage<KeyType> next_page;
  dt_file.seekg(page.getNext(), std::ios::beg);
  next_page.read(dt_file);

  // NextPage share with its records with the current page if it is possible
  if (page.getCount() + next_page.getCount() <=
      DataPage<KeyType>::getCapacity()) {
    // page.count = floor(N/2)-1 and  floor(N/2) <= next_page.count <=
    // floor(N/2)+1
    POS_TYPE page_count = page.getCount();
    for (int i = 0; i < next_page.getCount(); ++i) {
      page.setRecord(next_page.getRecord(i),
                     page_count + static_cast<POS_TYPE>(i));
    }
    page.setNext(next_page.getNext());
  }
  // The next page give it one record to current page
  else {
    // page.count = floor(N/2)-1 and next_page.count > floor(N/2) + 1
    page.setRecord(next_page.getRecord(0), page.getCount());
    next_page.remove(0);
    dt_file.seekp(page.getNext(), std::ios::beg);
    next_page.write(dt_file);
  }
}

template <typename KeyType>
void ISAM<KeyType>::insertDuplicate(ISAMRecord<KeyType> &rec,
                                    ISAMRecord<KeyType> &rec_dup) const {
  try {
    POS_TYPE duplicate_position;
    rec_dup.dup_pos = rec.dup_pos;
    this->insertDuplicateFile(rec_dup, duplicate_position);
    rec.dup_pos = duplicate_position;
  } catch (...) {
    throw std::runtime_error("Couldn't insert duplicate");
  }
}

template <typename KeyType>
void ISAM<KeyType>::insertDuplicateFile(ISAMRecord<KeyType> &rec,
                                        POS_TYPE &duplicate_position) const {
  if (!dup_file.is_open())
    throw std::runtime_error("Couldn't open duplicatesFile");

  dup_file.seekp(0, std::ios::end);
  duplicate_position = dup_file.tellp();

  try {
    dup_file.write((char *)&rec, sizeof(ISAMRecord<KeyType>));
  } catch (...) {
    throw std::runtime_error("Couldn't insert duplicate");
  }
}

template <typename KeyType>
void ISAM<KeyType>::getAllRawCurrentRecords(
    ISAMRecord<KeyType> record, std::vector<POS_TYPE> &records) const {
  if (!dup_file.is_open())
    throw std::runtime_error("Couldn't open duplicatesFile");

  try {
    records.push_back(record.pos);
    while (record.dup_pos != -1) {
      dup_file.seekp(record.dup_pos, std::ios::beg);
      dup_file.read(reinterpret_cast<char *>(&record),
                    sizeof(ISAMRecord<KeyType>));
      records.push_back(record.pos);
    }
  } catch (...) {
    throw std::runtime_error("Couldn't get all raw current records");
  }
}

template <typename KeyType>
std::string ISAM<KeyType>::get_attribute_name() const {
  return this->attribute_name;
}

template <typename KeyType> std::string ISAM<KeyType>::get_table_name() const {
  return this->table_name;
}

// template <typename KeyType> ISAM<KeyType>::~ISAM() {
//   std::cout << "Calling destructor" << std::endl;
//   close_files();
// }

template class ISAM<int>;
template class ISAM<float>;
