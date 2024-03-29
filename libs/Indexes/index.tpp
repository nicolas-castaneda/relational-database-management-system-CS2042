#ifndef INDEX_TPP
#define INDEX_TPP

template <typename KEY_TYPE>
template <typename RecordType>
void Index<KEY_TYPE>::getAllRawCurrentRecords(
    RecordType sir, std::vector<physical_pos> &records) const {
  std::fstream duplicatesFile(this->duplicatesFilename,
                              std::ios::in | std::ios::out | std::ios::binary);
  if (!duplicatesFile.is_open())
    throw std::runtime_error("Couldn't open duplicatesFile");

  try {
    records.push_back(sir.raw_pos);
    while (sir.dup_pos != -1) {
      this->moveReadRecord(duplicatesFile, sir.dup_pos, sir);
      records.push_back(sir.raw_pos);
    }
  } catch (...) {
    duplicatesFile.close();
    throw std::runtime_error("Couldn't get all raw current records");
  }
  duplicatesFile.close();
}

template <typename KEY_TYPE>
template <typename FileType, typename RecordType>
void Index<KEY_TYPE>::insertDuplicate(FileType &file, RecordType &rec,
                                      RecordType &rec_dup) const {
  try {
    physical_pos duplicate_position;
    rec_dup.setDupPos(rec.dup_pos);
    this->insertDuplicateFile(rec_dup, duplicate_position);
    rec.setDupPos(duplicate_position);
    this->moveWriteRecord(file, rec.current_pos, rec);
  } catch (...) {
    throw std::runtime_error("Couldn't insert duplicate");
  }
}

template <typename KEY_TYPE>
template <typename RecordType>
void Index<KEY_TYPE>::insertDuplicateFile(
    RecordType &rec, physical_pos &duplicate_position) const {
  std::fstream duplicatesFile(this->duplicatesFilename,
                              std::ios::in | std::ios::out | std::ios::binary);
  if (!duplicatesFile.is_open())
    throw std::runtime_error("Couldn't open duplicatesFile");

  duplicatesFile.seekp(0, std::ios::end);
  duplicate_position = duplicatesFile.tellp();

  try {
    this->writeRecord(duplicatesFile, rec);
  } catch (...) {
    duplicatesFile.close();
    throw std::runtime_error("Couldn't insert duplicate");
  }
  duplicatesFile.close();
}

template <typename KEY_TYPE>
template <typename HeaderType, typename RecordType>
size_t Index<KEY_TYPE>::numberRecordsWithHeader(std::string file_name) const {
  std::ifstream file(file_name, std::ios::in | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Couldn't open file");
  }
  file.seekg(0, std::ios::end);
  size_t size = static_cast<size_t>(file.tellg());
  file.close();
  return (size - sizeof(HeaderType)) / sizeof(RecordType);
}

template <typename KEY_TYPE>
template <typename HeaderType, typename RecordType>
void Index<KEY_TYPE>::printFileWithHeader(std::string file_name) const {
  std::fstream file(file_name, std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Couldn't open file");
  }
  HeaderType header;
  RecordType record;

  try {
    while (file.peek() != EOF) {
      if (file.tellg() == 0) {
        this->readHeader(file, header);
        std::cout << header << std::endl;
      } else {
        this->readRecord(file, record);
        std::cout << record << std::endl;
      }
    }
  } catch (...) {
    file.close();
    throw std::runtime_error("Couldn't print file with header");
  }
  file.close();
}

template <typename KEY_TYPE>
template <typename RecordType>
size_t Index<KEY_TYPE>::numberRecords(std::string file_name) const {
  std::ifstream file(file_name, std::ios::in | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Couldn't open file");
  }
  file.seekg(0, std::ios::end);
  size_t size = static_cast<size_t>(file.tellg());
  file.close();
  return size / sizeof(RecordType);
}

template <typename KEY_TYPE>
template <typename RecordType>
void Index<KEY_TYPE>::printFile(const std::string &file_name) const {
  std::fstream file(file_name, std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Couldn't open file");
  }
  RecordType record;

  try {
    while (file.peek() != EOF) {
      this->readRecord(file, record);
      std::cout << record << std::endl;
    }
  } catch (...) {
    file.close();
    throw std::runtime_error("Couldn't print file");
  }
  file.close();
}

template <typename KEY_TYPE>
template <typename FileType, typename HeaderType>
void Index<KEY_TYPE>::writeHeader(FileType &file, HeaderType &header) const {
  try {
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<char *>(&header), sizeof(HeaderType));
  } catch (...) {
    throw std::runtime_error("Couldn't write header");
  }
}

template <typename KEY_TYPE>
template <typename FileType, typename HeaderType>
void Index<KEY_TYPE>::readHeader(FileType &file, HeaderType &header) const {
  try {
    file.seekp(0, std::ios::beg);
    file.read(reinterpret_cast<char *>(&header), sizeof(HeaderType));
  } catch (...) {
    throw std::runtime_error("Couldn't read header");
  }
}

template <typename KEY_TYPE>
template <typename FileType, typename RecordType>
void Index<KEY_TYPE>::readRecord(FileType &file, RecordType &record) const {
  try {
    file.read(reinterpret_cast<char *>(&record), sizeof(RecordType));
  } catch (...) {
    throw std::runtime_error("Couldn't read record");
  }
}

template <typename KEY_TYPE>
template <typename FileType, typename RecordType>
void Index<KEY_TYPE>::writeRecord(FileType &file, RecordType &record) const {
  try {
    file.write(reinterpret_cast<char *>(&record), sizeof(RecordType));
  } catch (...) {
    throw std::runtime_error("Couldn't write record");
  }
}

template <typename KEY_TYPE>
template <typename FileType, typename RecordType>
void Index<KEY_TYPE>::moveReadRecord(FileType &file, physical_pos &pos,
                                     RecordType &record) const {
  try {
    file.seekp(pos, std::ios::beg);
    file.read(reinterpret_cast<char *>(&record), sizeof(RecordType));
  } catch (...) {
    throw std::runtime_error("Couldn't move read record");
  }
}

template <typename KEY_TYPE>
template <typename FileType, typename RecordType>
void Index<KEY_TYPE>::moveWriteRecord(FileType &file, physical_pos &pos,
                                      RecordType &record) const {
  try {
    file.seekp(pos, std::ios::beg);
    file.write(reinterpret_cast<char *>(&record), sizeof(RecordType));
  } catch (...) {
    throw std::runtime_error("Couldn't move write record");
  }
}

#endif // INDEX_TPP
