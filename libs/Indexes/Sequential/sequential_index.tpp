#ifndef SEQUENTIAL_INDEX_TPP
#define SEQUENTIAL_INDEX_TPP

#include "sequential_index.hpp"
using Index::Response;

template <typename KEY_TYPE> void SequentialIndex<KEY_TYPE>::rebuild() const {

  std::fstream indexFile(this->indexFilename,
                         std::ios::in | std::ios::out | std::ios::binary);
  if (!indexFile.is_open())
    throw std::runtime_error("Couldn't open indexFile");

  std::fstream auxFile(this->auxFilename,
                       std::ios::in | std::ios::out | std::ios::binary);
  if (!auxFile.is_open())
    throw std::runtime_error("Couldn't open auxFile");

  std::fstream duplicatesFile(this->duplicatesFilename,
                              std::ios::in | std::ios::out | std::ios::binary);
  if (!duplicatesFile.is_open())
    throw std::runtime_error("Couldn't open duplicatesFile");

  spdlog::info("Rebuilding index {}", this->indexFilename);

  std::ofstream newIndexFile(this->indexFilename + ".new",
                             std::ios::app | std::ios::binary);
  if (!newIndexFile.is_open())
    throw std::runtime_error("Couldn't create newIndexFile");

  spdlog::info("newIndexFile {}", this->indexFilename + ".new");

  std::ofstream newAuxFile(this->auxFilename + ".new",
                           std::ios::app | std::ios::binary);
  if (!newAuxFile.is_open())
    throw std::runtime_error("Couldn't create newAuxFile");

  std::ofstream newDuplicatesFile(this->duplicatesFilename + ".new",
                                  std::ios::app | std::ios::binary);
  if (!newDuplicatesFile.is_open())
    throw std::runtime_error("Couldn't create newDuplicatesFile");

  SequentialIndexHeader sih;
  SequentialIndexRecord<KEY_TYPE> sir;
  SequentialIndexRecord<KEY_TYPE> sir_dup;

  try {
    this->readHeader(indexFile, sih);
    if (sih.next_pos != END_OF_FILE) {
      SequentialIndexHeader new_sih(sizeof(SequentialIndexHeader), INDEXFILE);
      this->writeHeader(newIndexFile, new_sih);
      if (sih.next_file == INDEXFILE) {
        this->moveReadRecord(indexFile, sih.next_pos, sir);
      } else {
        this->moveReadRecord(auxFile, sih.next_pos, sir);
      }

      physical_pos next_move;
      physical_pos next_dup;
      file_pos next_file;

      while (true) {
        if (sir.dup_pos != END_OF_FILE) {
          physical_pos current_dup_pos = newDuplicatesFile.tellp();
          next_dup = sir.dup_pos;
          sir.setDupPos(current_dup_pos);
          this->moveReadRecord(duplicatesFile, next_dup, sir_dup);

          while (true) {
            current_dup_pos = newDuplicatesFile.tellp();
            sir_dup.setCurrent(current_dup_pos, DUPFILE);
            next_dup = sir_dup.dup_pos;
            if (next_dup == END_OF_FILE) {
              sir_dup.setDupPos(END_OF_FILE);
            } else {
              sir_dup.setDupPos(current_dup_pos +
                                static_cast<std::streamoff>(
                                    sizeof(SequentialIndexRecord<KEY_TYPE>)));
            }
            this->writeRecord(newDuplicatesFile, sir_dup);
            if (next_dup == END_OF_FILE) {
              break;
            }
            this->moveReadRecord(duplicatesFile, next_dup, sir_dup);
          }
        }
        physical_pos current_pos = newIndexFile.tellp();
        sir.setCurrent(current_pos, INDEXFILE);
        next_move = sir.next_pos;
        next_file = sir.next_file;
        if (next_move == END_OF_FILE) {
          sir.setNext(END_OF_FILE, INDEXFILE);
        } else {
          sir.setNext(current_pos + static_cast<std::streamoff>(sizeof(
                                        SequentialIndexRecord<KEY_TYPE>)),
                      INDEXFILE);
        }
        this->writeRecord(newIndexFile, sir);
        if (next_move == END_OF_FILE) {
          break;
        }
        if (next_file == INDEXFILE) {
          this->moveReadRecord(indexFile, next_move, sir);
        } else {
          this->moveReadRecord(auxFile, next_move, sir);
        }
      }
    } else {
      SequentialIndexHeader new_sih(END_OF_FILE, INDEXFILE);
      this->writeHeader(newIndexFile, new_sih);
    }

  } catch (...) {
    indexFile.close();
    auxFile.close();
    duplicatesFile.close();
    newIndexFile.close();
    newDuplicatesFile.close();
    throw std::runtime_error("Couldn't rebuild");
  }

  indexFile.close();
  auxFile.close();
  duplicatesFile.close();
  newIndexFile.close();
  newAuxFile.close();
  newDuplicatesFile.close();

  // bool rename_succes = true;
  //  truncate old files and replace new files with old names

  spdlog::info("Removing old files");

  std::filesystem::remove(this->indexFilename);

  // std::remove(this->indexFilename.c_str());

  spdlog::info("Removed file");

  std::filesystem::rename(this->indexFilename + ".new", this->indexFilename);

  // rename_succes = std::rename((this->indexFilename+".new").c_str(),
  //                             this->indexFilename.c_str());

  spdlog::info("Renamed file");

  std::filesystem::remove(this->auxFilename);

  // std::remove(this->auxFilename.c_str());

  std::filesystem::rename(this->auxFilename + ".new", this->auxFilename);

  // rename_succes = std::rename((this->auxFilename+".new").c_str(),
  //                       this->auxFilename.c_str());

  std::filesystem::remove(this->duplicatesFilename);

  // std::remove(this->duplicatesFilename.c_str());

  std::filesystem::rename(this->duplicatesFilename + ".new",
                          this->duplicatesFilename);

  // rename_succes = std::rename((this->duplicatesFilename+".new").c_str(),
  //                         this->duplicatesFilename.c_str());

  /*  if (!rename_succes) {
     throw std::runtime_error("Couldn't rename files");
   } */
}

/*
    Helper functions
*/

template <typename KEY_TYPE> void SequentialIndex<KEY_TYPE>::createFile() {
  std::ofstream indexFile(this->indexFilename,
                          std::ios::app | std::ios::binary);
  if (!indexFile.is_open()) {
    throw std::runtime_error("Couldn't create indexFile");
  }
  SequentialIndexHeader new_sih(END_OF_FILE, INDEXFILE);
  this->writeHeader(indexFile, new_sih);
  indexFile.close();

  std::ofstream auxFile(this->auxFilename, std::ios::app | std::ios::binary);
  if (!auxFile.is_open()) {
    throw std::runtime_error("Couldn't create auxFile");
  }
  auxFile.close();

  std::ofstream duplicatesFile(this->duplicatesFilename,
                               std::ios::app | std::ios::binary);
  if (!duplicatesFile.is_open()) {
    throw std::runtime_error("Couldn't create duplicatesFile");
  }
  duplicatesFile.close();
}

template <typename KEY_TYPE> bool SequentialIndex<KEY_TYPE>::fileExists() {
  std::ifstream file(this->indexFilename);
  bool exists = file.good();
  file.close();
  return exists;
}

/*
    Binary search in files
*/

template <typename KEY_TYPE>
template <typename FileType>
BinarySearchResponse<KEY_TYPE>
SequentialIndex<KEY_TYPE>::binarySearch(FileType &file,
                                        Data<KEY_TYPE> data) const {
  BinarySearchResponse<KEY_TYPE> bsr;
  std::streamoff header_offset = sizeof(SequentialIndexHeader);
  std::streamoff sequentialIndexSize = sizeof(SequentialIndexRecord<KEY_TYPE>);

  try {
    physical_pos logical_left = 0;

    file.seekp(0, std::ios::end);
    if (file.tellp() == header_offset) {
      bsr.location = EMPTY_FILE;
      return bsr;
    }
    physical_pos physical_last = file.tellp();
    physical_pos logical_right =
        (physical_last - header_offset) / sequentialIndexSize;
    file.seekp(0, std::ios::beg);

    SequentialIndexHeader sih;
    SequentialIndexRecord<KEY_TYPE> sir_prev;
    SequentialIndexRecord<KEY_TYPE> sir_cur;
    SequentialIndexRecord<KEY_TYPE> sir_next;

    while (logical_left <= logical_right) {
      physical_pos logical_mid = (logical_left + logical_right) / 2;
      physical_pos physical_mid =
          header_offset + (logical_mid * sequentialIndexSize);

      file.seekp(physical_mid, std::ios::beg);
      this->readRecord(file, sir_cur);
      if (sir_cur.current_pos != header_offset) {
        file.seekp(-2 * sequentialIndexSize, std::ios::cur);
        this->readRecord(file, sir_prev);
      } else {
        this->readHeader(file, sih);
      }
      if (sir_cur.current_pos + sequentialIndexSize != physical_last) {
        file.seekp(sequentialIndexSize, std::ios::cur);
        this->readRecord(file, sir_next);
      }

      if (sir_cur.data == data) {
        bsr.location = REC_CUR;
        break;
      } else if (sir_cur.current_pos + sequentialIndexSize == physical_last &&
                 sir_cur.data < data) {
        bsr.location = REC_NEXT;
        break;
      } else if (sir_cur.current_pos == header_offset && data < sir_cur.data) {
        bsr.location = REC_PREV;
        bsr.setHeader(sih);
        break;
      } else if (sir_prev.data < data && data < sir_cur.data) {
        bsr.location = REC_PREV;
        break;
      } else if (sir_cur.data < data && data < sir_next.data) {
        bsr.location = REC_NEXT;
        break;
      } else if (sir_cur.data < data) {
        logical_left = logical_mid + 1L;
      } else {
        logical_right = logical_mid - 1L;
      }
    }
    bsr.setRecords(sir_prev, sir_cur, sir_next);
  } catch (...) {
    throw std::runtime_error("Couldn't binary search");
  }
  return bsr;
}

/*
    Query functions
*/

template <typename KEY_TYPE>
void SequentialIndex<KEY_TYPE>::insertAuxFile(
    SequentialIndexRecord<KEY_TYPE> &sir) const {
  std::fstream auxFile(this->auxFilename,
                       std::ios::in | std::ios::out | std::ios::binary);
  if (!auxFile.is_open()) {
    throw std::runtime_error("Couldn't open auxFile");
  }

  auxFile.seekp(0, std::ios::end);
  physical_pos physical_pos = auxFile.tellp();

  sir.setCurrent(physical_pos, AUXFILE);

  try {
    this->writeRecord(auxFile, sir);
  } catch (...) {
    auxFile.close();
    throw std::runtime_error("Couldn't insert auxFile");
  }

  auxFile.close();
};

template <typename KEY_TYPE>
template <typename FileType>
void SequentialIndex<KEY_TYPE>::insertAfterRecord(
    FileType &file, SequentialIndexRecord<KEY_TYPE> &sir_prev,
    SequentialIndexRecord<KEY_TYPE> &sir, SequentialIndexHeader &sih,
    bool header) const {
  try {
    if (header) {
      sir.setNext(sih.next_pos, sih.next_file);
      this->insertAuxFile(sir);
      SequentialIndexHeader new_sih(sir.current_pos, sir.current_file);
      this->writeHeader(file, new_sih);
    } else {
      sir.setNext(sir_prev.next_pos, sir_prev.next_file);
      this->insertAuxFile(sir);
      sir_prev.setNext(sir.current_pos, sir.current_file);
      file.seekp(sir_prev.current_pos, std::ios::beg);
      this->writeRecord(file, sir_prev);
    }
  } catch (...) {
    throw std::runtime_error("Couldn't insert between records");
  }
}

template <typename KEY_TYPE>
template <typename FileType>
void SequentialIndex<KEY_TYPE>::insertAux(
    FileType &indexFile, SequentialIndexRecord<KEY_TYPE> &sir_init,
    SequentialIndexRecord<KEY_TYPE> &sir,
    BinarySearchResponse<KEY_TYPE> &bsr) const {
  std::fstream auxFile(this->auxFilename,
                       std::ios::in | std::ios::out | std::ios::binary);
  if (!auxFile.is_open()) {
    throw std::runtime_error("Couldn't open auxFile");
  }

  SequentialIndexRecord<KEY_TYPE> sir_prev = sir_init;
  SequentialIndexRecord<KEY_TYPE> sir_cur;

  try {
    if (bsr.header) {
      auxFile.seekp(bsr.sih.next_pos, std::ios::beg);
    } else {
      auxFile.seekp(sir_prev.next_pos, std::ios::beg);
    }
    this->readRecord(auxFile, sir_cur);

    bool inserted = false;

    if (bsr.header) {
      if (sir.data < sir_cur.data) {
        this->insertAfterRecord(auxFile, sir_init, sir, bsr.sih, bsr.header);
        inserted = true;
      } else if (sir.data == sir_cur.data) {
        if (this->PK)
          throw std::runtime_error("Couldn't add duplicate in PK");
        this->insertDuplicate(auxFile, sir_cur, sir);
        inserted = true;
      } else if (sir_cur.next_file == INDEXFILE) {
        this->insertAfterRecord(auxFile, sir_cur, sir, bsr.sih, !bsr.header);
        inserted = true;
      } else {
        sir_prev = sir_cur;
        this->moveReadRecord(auxFile, sir_cur.next_pos, sir_cur);
        bsr.header = false;
      }
    } else if (sir_prev.data < sir.data && sir.data < sir_cur.data) {
      this->insertAfterRecord(indexFile, sir_prev, sir, bsr.sih, bsr.header);
      inserted = true;
    }

    if (!inserted) {
      while (sir_cur.next_file != INDEXFILE) {
        if (sir_cur.data == sir.data) {
          if (this->PK) {
            throw std::runtime_error("Couldn't add duplicate in PK");
          }
          this->insertDuplicate(auxFile, sir_cur, sir);
          break;
        }
        if (sir_prev.data < sir.data && sir.data < sir_cur.data) {
          this->insertAfterRecord(auxFile, sir_prev, sir, bsr.sih, bsr.header);
          break;
        }
        sir_prev = sir_cur;
        this->moveReadRecord(auxFile, sir_cur.next_pos, sir_cur);
      }

      if (sir_cur.next_file == INDEXFILE) {
        if (sir_cur.data == sir.data) {
          if (this->PK) {
            throw std::runtime_error("Couldn't add duplicate in PK");
          }
          this->insertDuplicate(auxFile, sir_cur, sir);
        } else if (sir_cur.data < sir.data) {
          this->insertAfterRecord(auxFile, sir_cur, sir, bsr.sih, bsr.header);
        } else {
          this->insertAfterRecord(auxFile, sir_prev, sir, bsr.sih, bsr.header);
        }
      }
    }
  } catch (...) {
    auxFile.close();
    throw std::runtime_error("Couldn't insert auxFile");
  }

  auxFile.close();
}

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::add(Data<KEY_TYPE> data,
                                        physical_pos raw_pos,
                                        bool rebuild) const {

  spdlog::info("Adding {} to index {}", data,
               static_cast<std::streamoff>(raw_pos));
  Response response;
  response.startTimer();

  std::fstream indexFile(this->indexFilename,
                         std::ios::in | std::ios::out | std::ios::binary);
  if (!indexFile.is_open()) {
    throw std::runtime_error("Couldn't open indexFile");
  }

  SequentialIndexRecord<KEY_TYPE> sir;
  sir.setData(data);
  sir.setRawPos(raw_pos);
  sir.setDupPos(END_OF_FILE);

  try {
    BinarySearchResponse bsr = this->binarySearch(indexFile, data);
    if (bsr.location == EMPTY_FILE) {
      // Insert as first record
      sir.setCurrent(sizeof(SequentialIndexHeader), INDEXFILE);
      sir.setNext(END_OF_FILE, INDEXFILE);
      SequentialIndexHeader new_sih(sizeof(SequentialIndexHeader), INDEXFILE);
      this->writeHeader(indexFile, new_sih);
      this->writeRecord(indexFile, sir);
      response.records.push_back(sir.raw_pos);
    } else if (bsr.header && bsr.sih.next_file == INDEXFILE) {
      // Insert between header and first record
      this->insertAfterRecord(indexFile, bsr.sir_prev, sir, bsr.sih,
                              bsr.header);
    } else if (bsr.header) {
      // Move to aux file from header
      this->insertAux(indexFile, bsr.sir_prev, sir, bsr);
    } else if (bsr.location == REC_PREV &&
               bsr.sir_prev.next_file == INDEXFILE) {
      // Insert between prev and cur
      this->insertAfterRecord(indexFile, bsr.sir_prev, sir, bsr.sih,
                              bsr.header);
    } else if (bsr.location == REC_PREV) {
      // Move to aux file from prev
      this->insertAux(indexFile, bsr.sir_prev, sir, bsr);
    } else if (bsr.location == REC_CUR) {
      // Insert duplicate
      if (this->PK) {
        throw std::runtime_error("Couldn't add duplicate in PK");
      }
      this->insertDuplicate(indexFile, bsr.sir, sir);
    } else if (bsr.sir.next_file == INDEXFILE) {
      // Insert between cur and next
      spdlog::info("Inserting between cur and next");
      this->insertAfterRecord(indexFile, bsr.sir, sir, bsr.sih, bsr.header);
    } else {
      // Move to aux file from cur
      this->insertAux(indexFile, bsr.sir, sir, bsr);
    }

  } catch (std::runtime_error &) {
    response.stopTimer();
    indexFile.close();
    throw std::runtime_error("Couldn't add");
  }

  response.records.push_back(sir.raw_pos);
  response.stopTimer();
  indexFile.close();

  /* std::fstream ifile(this->indexFilename, std::ios::in | std::ios::out |
  std::ios::binary); SequentialIndexHeader h; this->readHeader(ifile, h);
  std::cout<<"HEADER"<<std::endl;
  std::cout<<h<<std::endl;
  SequentialIndexRecord<KEY_TYPE> r;
  std::cout<<"RECORDS"<<std::endl;
  this->moveReadRecord(ifile, h.next_pos, r);
  std::cout<<r<<std::endl;
  while(r.next_file != INDEXFILE){
    this->moveReadRecord(ifile, r.next_pos, r);
    std::cout<<r<<std::endl;
  }
  ifile.close(); */

  if (rebuild && !this->validNumberRecords()) {
    this->rebuild();
  }

  /* std::cout<<"INDEXFILE"<<std::endl;
  this->template printFileWithHeader<SequentialIndexHeader,
  SequentialIndexRecord<KEY_TYPE>>(this->indexFilename);
  std::cout<<"AUXFILE"<<std::endl;
  this->template printFile<SequentialIndexRecord<KEY_TYPE>>(this->auxFilename);
*/

  /* std::fstream ifile(this->indexFilename, std::ios::in | std::ios::out |
  std::ios::binary); SequentialIndexHeader h; this->readHeader(ifile, h);
  std::cout<<"HEADER"<<std::endl;
  std::cout<<h<<std::endl;
  SequentialIndexRecord<KEY_TYPE> r;
  std::cout<<"RECORDS"<<std::endl;
  this->moveReadRecord(ifile, h.next_pos, r);
  std::cout<<r<<std::endl;
  while(r.next_file != INDEXFILE){
    this->moveReadRecord(ifile, r.next_pos, r);
    std::cout<<r<<std::endl;
  }
  ifile.close();  */

  // thi

  return response;
}

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::add(Data<KEY_TYPE> data,
                                        physical_pos raw_pos) const {
  Response response;
  try {
    response = this->add(data, raw_pos, true);
  } catch (...) {
    throw std::runtime_error("Couldn't add");
  }
  return response;
};

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::addNotRebuild(Data<KEY_TYPE> data,
                                                  physical_pos raw_pos) {
  Response response;
  try {
    response = this->add(data, raw_pos, false);
  } catch (...) {
    throw std::runtime_error("Couldn't add not rebuild");
  }
  return response;
}

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::loadRecords(
    std::vector<std::pair<Data<KEY_TYPE>, physical_pos>> &records) {
  Response response;
  response.startTimer();
  try {
    for (auto &record : records) {
      this->addNotRebuild(record.first, record.second);
    }
  } catch (...) {
    response.stopTimer();
    throw std::runtime_error("Couldn't load records");
  }
  response.stopTimer();
  this->rebuild();
  return response;
}

template <typename KEY_TYPE>
void SequentialIndex<KEY_TYPE>::searchAuxFile(
    Data<KEY_TYPE> data, BinarySearchResponse<KEY_TYPE> &bir,
    std::vector<physical_pos> &records,
    SequentialIndexRecord<KEY_TYPE> &sir) const {
  std::fstream auxFile(this->auxFilename,
                       std::ios::in | std::ios::out | std::ios::binary);
  if (!auxFile.is_open())
    throw std::runtime_error("Couldn't open auxFile");

  try {
    if (bir.header) {
      sir = bir.sir;
      if (bir.sih.next_file == INDEXFILE)
        return;
      auxFile.seekp(bir.sih.next_pos, std::ios::beg);
    } else if (bir.location == REC_PREV) {
      sir = bir.sir_prev;
      if (bir.sir_prev.next_file == INDEXFILE)
        return;
      auxFile.seekp(bir.sir_prev.next_pos, std::ios::beg);
    } else if (bir.location == REC_NEXT) {
      sir = bir.sir;
      if (bir.sir.next_file == INDEXFILE)
        return;
      auxFile.seekp(bir.sir.next_pos, std::ios::beg);
    }
    this->readRecord(auxFile, sir);
    while (data >= sir.data) {
      std::cout << sir << std::endl;
      if (sir.data == data) {
        this->getAllRawCurrentRecords(sir, records);
        break;
      }
      if (sir.next_file == INDEXFILE || sir.next_pos == END_OF_FILE) {
        break;
      }
      auxFile.seekp(sir.next_pos, std::ios::beg);
      this->readRecord(auxFile, sir);
    }
  } catch (...) {
    auxFile.close();
    throw std::runtime_error("Couldn't search auxFile");
  }
  auxFile.close();
}

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::search(Data<KEY_TYPE> data) const {
  Response response;

  std::fstream indexFile(this->indexFilename,
                         std::ios::in | std::ios::out | std::ios::binary);
  if (!indexFile.is_open()) {
    throw std::runtime_error("Couldn't open indexFile");
  }

  response.startTimer();
  try {
    BinarySearchResponse<KEY_TYPE> bsr = this->binarySearch(indexFile, data);
    SequentialIndexRecord<KEY_TYPE> sir;
    if (bsr.location == EMPTY_FILE) {
      response.stopTimer();
      indexFile.close();
      return response;
    }
    if (bsr.location == REC_CUR) {
      this->getAllRawCurrentRecords(bsr.sir, response.records);
    } else {
      this->searchAuxFile(data, bsr, response.records, sir);
    }
  } catch (std::runtime_error &) {
    response.stopTimer();
    indexFile.close();
    throw std::runtime_error("Couldn't search");
  }

  response.stopTimer();
  indexFile.close();
  return response;
}

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::range_search(Data<KEY_TYPE> begin,
                                                 Data<KEY_TYPE> end) const {
  Response response;

  std::fstream indexFile(this->indexFilename,
                         std::ios::in | std::ios::out | std::ios::binary);
  if (!indexFile.is_open()) {
    throw std::runtime_error("Couldn't open indexFile");
  }

  response.startTimer();
  if (begin > end) {
    response.stopTimer();
    indexFile.close();
    return response;
  }
  try {
    BinarySearchResponse<KEY_TYPE> bsr = this->binarySearch(indexFile, begin);
    SequentialIndexRecord<KEY_TYPE> sir;

    if (bsr.location == EMPTY_FILE) {
      response.stopTimer();
      indexFile.close();
      return response;
    } else if (bsr.location == REC_CUR) {
      sir = bsr.sir;
      this->getAllRawCurrentRecords(sir, response.records);
    } else {
      this->searchAuxFile(begin, bsr, response.records, sir);
    }
    if (sir.data <= begin) {
      // Move to the next record
      if (sir.next_file == INDEXFILE) {
        if (sir.next_pos == END_OF_FILE) {
          response.stopTimer();
          indexFile.close();
          return response;
        }
        this->moveReadRecord(indexFile, sir.next_pos, sir);
      } else {
        std::fstream auxFile(this->auxFilename,
                             std::ios::in | std::ios::out | std::ios::binary);
        if (!auxFile.is_open())
          throw std::runtime_error("Couldn't open auxFile");
        auxFile.seekp(sir.next_pos, std::ios::beg);
        this->readRecord(auxFile, sir);
        auxFile.close();
      }
    }
    std::fstream auxFile(this->auxFilename,
                         std::ios::in | std::ios::out | std::ios::binary);
    if (!auxFile.is_open()) {
      throw std::runtime_error("Couldn't open auxFile");
    }
    while (sir.data <= end) {

      this->getAllRawCurrentRecords(sir, response.records);
      if (sir.next_pos == END_OF_FILE) {
        break;
      }
      if (sir.next_file == INDEXFILE) {
        this->moveReadRecord(indexFile, sir.next_pos, sir);
      } else {
        this->moveReadRecord(auxFile, sir.next_pos, sir);
      }
    }
    auxFile.close();
  } catch (std::runtime_error &) {
    response.stopTimer();
    indexFile.close();
    throw std::runtime_error("Couldn't search");
  }

  response.stopTimer();
  indexFile.close();
  return response;
}

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::erase(Data<KEY_TYPE> data,
                                          Response &response) const {

  std::fstream indexFile(this->indexFilename,
                         std::ios::in | std::ios::out | std::ios::binary);
  if (!indexFile.is_open()) {
    throw std::runtime_error("Couldn't open indexFile");
  }

  response.startTimer();
  try {
    BinarySearchResponse bsr = this->binarySearch(indexFile, data);

    if (bsr.location == EMPTY_FILE) {
      response.stopTimer();
      indexFile.close();
      return response;
    }
    if (bsr.location == REC_CUR) {
      if (bsr.sir.current_pos == sizeof(SequentialIndexHeader)) {
        SequentialIndexHeader sih;
        this->readHeader(indexFile, sih);
        if (sih.next_file == INDEXFILE) {

          sih.setNext(bsr.sir.next_pos, bsr.sir.next_file);
          bsr.sir.setNext(END_OF_FILE, INDEXFILE);

          this->writeHeader(indexFile, sih);
          this->moveWriteRecord(indexFile, bsr.sir.current_pos, bsr.sir);
        } else {
          std::fstream auxFile(this->auxFilename,
                               std::ios::in | std::ios::out | std::ios::binary);
          if (!auxFile.is_open()) {
            throw std::runtime_error("Couldn't open auxFile");
          }

          SequentialIndexRecord<KEY_TYPE> sir_cur;
          this->moveReadRecord(auxFile, sih.next_pos, sir_cur);
          while (sir_cur.next_file != INDEXFILE) {
            this->moveReadRecord(auxFile, sir_cur.next_pos, sir_cur);
          }

          sir_cur.setNext(bsr.sir.next_pos, bsr.sir.next_file);
          bsr.sir.setNext(END_OF_FILE, INDEXFILE);

          this->moveWriteRecord(auxFile, sir_cur.current_pos, sir_cur);

          auxFile.close();
        }
        response.records.push_back(bsr.sir.raw_pos);
        response.stopTimer();
        indexFile.close();
        return response;
      }
      if (bsr.sir_prev.next_file == INDEXFILE) {
        bsr.sir_prev.setNext(bsr.sir.next_pos, bsr.sir.next_file);
        bsr.sir.setNext(END_OF_FILE, INDEXFILE);

        this->moveWriteRecord(indexFile, bsr.sir_prev.current_pos,
                              bsr.sir_prev);
        this->moveWriteRecord(indexFile, bsr.sir.current_pos, bsr.sir);

        response.records.push_back(bsr.sir.raw_pos);
        response.stopTimer();
        indexFile.close();
        return response;
      }
      std::fstream auxFile(this->auxFilename,
                           std::ios::in | std::ios::out | std::ios::binary);
      if (!auxFile.is_open()) {
        throw std::runtime_error("Couldn't open auxFile");
      }

      SequentialIndexRecord<KEY_TYPE> sir_cur;
      this->moveReadRecord(auxFile, bsr.sir_prev.next_pos, sir_cur);
      while (sir_cur.next_file != INDEXFILE) {
        this->moveReadRecord(auxFile, sir_cur.next_pos, sir_cur);
      }

      sir_cur.setNext(bsr.sir.next_pos, bsr.sir.next_file);
      bsr.sir.setNext(END_OF_FILE, INDEXFILE);

      this->moveWriteRecord(auxFile, sir_cur.current_pos, sir_cur);
      this->moveWriteRecord(indexFile, bsr.sir.current_pos, bsr.sir);

      response.records.push_back(bsr.sir.raw_pos);
      response.stopTimer();
      indexFile.close();
      auxFile.close();
      return response;

    } else {
      std::fstream auxFile(this->auxFilename,
                           std::ios::in | std::ios::out | std::ios::binary);
      if (!auxFile.is_open())
        throw std::runtime_error("Couldn't open auxFile");
      SequentialIndexRecord<KEY_TYPE> sir_cur;
      SequentialIndexRecord<KEY_TYPE> sir_prev;
      // Record to delete is not in files
      if ((bsr.header && bsr.sih.next_file == INDEXFILE) ||
          (bsr.location == REC_PREV && bsr.sir_prev.next_file == INDEXFILE) ||
          (bsr.location == REC_NEXT && bsr.sir.next_file == INDEXFILE) ||
          (bsr.header && bsr.sih.next_pos == END_OF_FILE) ||
          (bsr.location == REC_PREV && bsr.sir_prev.next_pos == END_OF_FILE) ||
          (bsr.location == REC_NEXT && bsr.sir.next_pos == END_OF_FILE)) {
        response.stopTimer();
        indexFile.close();
        auxFile.close();
        return response;
      }
      if (bsr.header) {

        this->moveReadRecord(indexFile, bsr.sih.next_pos, sir_cur);
        if (sir_cur.data == data) {
          bsr.sih.next_pos = sir_cur.next_pos;
          bsr.sih.next_file = sir_cur.next_file;
          sir_cur.setNext(END_OF_FILE, INDEXFILE);
          SequentialIndexHeader new_sih(bsr.sih.next_pos, bsr.sih.next_file);
          this->writeHeader(indexFile, new_sih);
          this->moveWriteRecord(indexFile, sir_cur.current_pos, sir_cur);
          auxFile.close();
          response.stopTimer();
          indexFile.close();
          return response;
        }
        if (sir_cur.next_file == INDEXFILE || sir_cur.next_pos == END_OF_FILE ||
            sir_cur.data > data) {
          response.stopTimer();
          indexFile.close();
          auxFile.close();
          return response;
        }
        sir_prev = sir_cur;
        this->moveReadRecord(auxFile, sir_cur.next_pos, sir_cur);

      } else if (bsr.location == REC_PREV) {
        sir_prev = bsr.sir_prev;
        this->moveReadRecord(auxFile, sir_prev.next_pos, sir_cur);
      } else if (bsr.location == REC_NEXT) {
        sir_prev = bsr.sir;
        this->moveReadRecord(auxFile, sir_prev.next_pos, sir_cur);
      }
      while (true) {
        if (sir_cur.data == data) {
          sir_prev.setNext(sir_cur.next_pos, sir_cur.next_file);
          sir_cur.setNext(END_OF_FILE, INDEXFILE);

          if (sir_prev.current_file == INDEXFILE) {
            this->moveWriteRecord(indexFile, sir_prev.current_pos, sir_prev);
          } else {
            this->moveWriteRecord(auxFile, sir_prev.current_pos, sir_prev);
          }
          this->moveWriteRecord(auxFile, sir_cur.current_pos, sir_cur);
          break;
        }
        if (sir_cur.next_file == INDEXFILE || sir_cur.next_pos == END_OF_FILE ||
            sir_cur.data > data) {
          response.stopTimer();
          indexFile.close();
          auxFile.close();
          return response;
        }
        sir_prev = sir_cur;
        this->moveReadRecord(auxFile, sir_cur.next_pos, sir_cur);
      }
      response.records.push_back(sir_cur.raw_pos);
    }
  } catch (std::runtime_error &) {
    response.stopTimer();
    indexFile.close();
    throw std::runtime_error("Couldn't search");
  }

  response.stopTimer();
  indexFile.close();
  return response;
}

template <typename KEY_TYPE>
Response SequentialIndex<KEY_TYPE>::remove(Data<KEY_TYPE> data) const {
  Response response;
  this->erase(data, response);
  if (!response.records.empty()) {
    this->rebuild();
  }
  return response;
}

template <typename KEY_TYPE>
std::pair<Response, std::vector<bool>> SequentialIndex<KEY_TYPE>::bulk_insert(
    const std::vector<std::pair<Data<KEY_TYPE>, physical_pos>> &records) const {

  Response response;
  response.startTimer();
  try {
    if (records.empty()) {
      response.stopTimer();
      return {response, {}};
    }
    auto record_copy = records;

    std::sort(record_copy.begin(), record_copy.end(),
              [](std::pair<Data<KEY_TYPE>, physical_pos> a,
                 std::pair<Data<KEY_TYPE>, physical_pos> b) {
                return a.first < b.first;
              });

    std::fstream indexFile(this->indexFilename,
                           std::ios::in | std::ios::out | std::ios::binary);
    if (!indexFile.is_open()) {
      throw std::runtime_error("Couldn't open indexFile");
    }

    indexFile.seekp(0, std::ios::end);
    physical_pos physical_pos = indexFile.tellp();

    if (physical_pos == sizeof(SequentialIndexHeader)) {
      SequentialIndexHeader sih(sizeof(SequentialIndexHeader), INDEXFILE);
      this->writeHeader(indexFile, sih);

      SequentialIndexRecord<KEY_TYPE> sir_prev;
      SequentialIndexRecord<KEY_TYPE> sir_cur;
      for (size_t i = 0; i < record_copy.size(); i++) {
        sir_cur.setData(record_copy[i].first);
        sir_cur.setRawPos(record_copy[i].second);
        sir_cur.setDupPos(END_OF_FILE);
        if (i == 0) {
          sir_cur.setCurrent(sizeof(SequentialIndexHeader), INDEXFILE);
          sir_cur.setNext(END_OF_FILE, INDEXFILE);
          this->moveWriteRecord(indexFile, sir_cur.current_pos, sir_cur);
          sir_prev = sir_cur;
        } else {
          if (sir_cur.data == sir_prev.data) {
            this->insertDuplicate(indexFile, sir_prev, sir_cur);
          } else {
            sir_cur.setCurrent(sir_prev.current_pos +
                                   static_cast<std::streamoff>(
                                       sizeof(SequentialIndexRecord<KEY_TYPE>)),
                               INDEXFILE);
            sir_cur.setNext(END_OF_FILE, INDEXFILE);
            sir_prev.setNext(sir_cur.current_pos, sir_cur.current_file);
            this->moveWriteRecord(indexFile, sir_prev.current_pos, sir_prev);
            this->moveWriteRecord(indexFile, sir_cur.current_pos, sir_cur);
            sir_prev = sir_cur;
          }
        }
        response.records.push_back(sir_cur.raw_pos);
      }
    }
    indexFile.close();
  } catch (...) {
    response.stopTimer();
    throw std::runtime_error("Couldn't load records");
  }

  response.stopTimer();
  return {response, std::vector<bool>(records.size())};
}

template <typename KEY_TYPE>
std::string SequentialIndex<KEY_TYPE>::get_index_name() const {
  return this->index_name;
}

/*
    Print files sequentially
*/
template <typename KEY_TYPE>
void SequentialIndex<KEY_TYPE>::printIndexFile() const {
  try {
    this->template printFileWithHeader<SequentialIndexHeader,
                                       SequentialIndexRecord<KEY_TYPE>>(
        this->indexFilename);
  } catch (std::runtime_error &) {
    throw std::runtime_error("Couldn't print index file");
  }
}

template <typename KEY_TYPE>
void SequentialIndex<KEY_TYPE>::printAuxFile() const {
  try {
    this->template printFile<SequentialIndexRecord<KEY_TYPE>>(
        this->auxFilename);
  } catch (std::runtime_error &) {
    throw std::runtime_error("Couldn't print aux file");
  }
}

template <typename KEY_TYPE>
void SequentialIndex<KEY_TYPE>::printDuplicatesFile() const {
  try {
    this->template printFile<SequentialIndexRecord<KEY_TYPE>>(
        this->duplicatesFilename);
  } catch (std::runtime_error) {
    throw std::runtime_error("Couldn't print duplicates file");
  }
}

template <typename KEY_TYPE>
bool SequentialIndex<KEY_TYPE>::validNumberRecords() const {
  size_t indexRecords =
      this->template numberRecordsWithHeader<SequentialIndexHeader,
                                             SequentialIndexRecord<KEY_TYPE>>(
          this->indexFilename);
  size_t auxRecords =
      this->template numberRecords<SequentialIndexRecord<KEY_TYPE>>(
          this->auxFilename);
  return log2(static_cast<double>(indexRecords)) >=
         static_cast<double>(auxRecords);
}

#endif // SEQUENTIAL_INDEX_TPP
