#ifndef AVL_INDEX_CPP
#define AVL_INDEX_CPP

#include "avl_index.hpp"
#include <cstdint>

using Index::Response;

template <typename KEY_TYPE> void AVLIndex<KEY_TYPE>::initFile() {
  file.open(this->indexFileName, std::ios::in | std::ios::binary);
  if (!file.is_open()) {
    file.open(this->indexFileName, std::ios::out | std::ios::binary);
    if (!file.good()) {
      throw std::runtime_error("No se pudo crear el AVLIndexFile!");
    }
    file.close();
  } else {
    file.close();
  }

  std::ofstream duplicatesFile(this->duplicatesFilename,
                               std::ios::app | std::ios::binary);
  if (!duplicatesFile.is_open()) {
    throw std::runtime_error("Couldn't create duplicatesFile");
  }
  duplicatesFile.close();
}

//*
template <typename KEY_TYPE> void AVLIndex<KEY_TYPE>::initIndex() {
  
  initFile();
  file.open(this->indexFileName,
            std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("No se pudo abrir el archivo AVLIndex!");
  }
  file.seekg(0, std::ios::end);
  if (file.tellg() < sizeof(AVLIndexHeader)) {
    this->header.rootPointer = -1;
    this->header.lastDelete = -1;
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<char *>(&header), sizeof(AVLIndexHeader));
  } else {
    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char *>(&this->header), sizeof(AVLIndexHeader));
  }

  file.close();
}

template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::insert(physical_pos cPointer,
                                AVLIndexNode<KEY_TYPE> &cNode,
                                AVLIndexNode<KEY_TYPE> &iNode,
                                Response &response) const {
  if (cPointer == -1) {
    file.seekp(sizeof(AVLIndexHeader), std::ios::beg);

    iNode.current_pos = sizeof(AVLIndexHeader);
    file.write(reinterpret_cast<char *>(&iNode),
               sizeof(AVLIndexNode<KEY_TYPE>));

    header.rootPointer = sizeof(AVLIndexHeader);

    file.seekp(0, std::ios::beg);

    file.write(reinterpret_cast<char *>(&header), sizeof(AVLIndexHeader));
    return;
  }

  file.seekg(cPointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));

  if (iNode.data > cNode.data) {
    if (cNode.rightChildren != -1) {
      insert(cNode.rightChildren, cNode, iNode, response);
    } else {
      file.seekg(0, std::ios::end);
      physical_pos insertPointer = file.tellg();
      if (header.lastDelete != -1) {
        insertPointer = header.lastDelete;
        AVLIndexNode<KEY_TYPE> tempNode;
        file.seekg(insertPointer, std::ios::beg);
        file.read(reinterpret_cast<char *>(&tempNode),
                  sizeof(AVLIndexNode<KEY_TYPE>));

        header.lastDelete = tempNode.nextDelete;
        file.seekp(0, std::ios::beg);
        file.write(reinterpret_cast<char *>(&header), sizeof(AVLIndexHeader));
      }

      file.seekp(insertPointer, std::ios::beg);
      iNode.current_pos = insertPointer;
      file.write(reinterpret_cast<char *>(&iNode),
                 sizeof(AVLIndexNode<KEY_TYPE>));

      cNode.rightChildren = insertPointer;
      file.seekp(cPointer, std::ios::beg);
      cNode.current_pos = cPointer;
      file.write(reinterpret_cast<char *>(&cNode),
                 sizeof(AVLIndexNode<KEY_TYPE>));
    }
  } else if (iNode.data < cNode.data) {
    if (cNode.leftChildren != -1) {
      insert(cNode.leftChildren, cNode, iNode, response);
    } else {
      file.seekg(0, std::ios::end);
      physical_pos insertPointer = file.tellg();
      if (header.lastDelete != -1) {
        insertPointer = header.lastDelete;
        AVLIndexNode<KEY_TYPE> tempNode;
        file.seekg(insertPointer, std::ios::beg);
        file.read(reinterpret_cast<char *>(&tempNode),
                  sizeof(AVLIndexNode<KEY_TYPE>));

        header.lastDelete = tempNode.nextDelete;
        file.seekp(0, std::ios::beg);
        file.write(reinterpret_cast<char *>(&header), sizeof(AVLIndexHeader));
      }

      file.seekp(insertPointer, std::ios::beg);
      iNode.current_pos = insertPointer;
      file.write(reinterpret_cast<char *>(&iNode),
                 sizeof(AVLIndexNode<KEY_TYPE>));

      cNode.leftChildren = insertPointer;
      file.seekp(cPointer, std::ios::beg);
      cNode.current_pos = cPointer;
      file.write(reinterpret_cast<char *>(&cNode),
                 sizeof(AVLIndexNode<KEY_TYPE>));
    }
  } else {
    if (this->PK)
      throw std::runtime_error("Couldn't add duplicate in PK");
    this->insertDuplicate(file, cNode, iNode);
    return;
  }

  updateHeigth(cPointer);
  if (!isBalanced(cPointer)) {
    balance(cPointer);
  }
  return;
}

//*
template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::balance(physical_pos nodePointer) const {
  if (nodePointer == -1) {
    return;
  }
  AVLIndexNode<KEY_TYPE> node;
  file.seekg(nodePointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&node), sizeof(AVLIndexNode<KEY_TYPE>));

  int balance = balancingFactor(nodePointer);

  if (balance > 1) // Esta cargado a la izquierda?
  {
    if (balancingFactor(node.leftChildren) <= -1) {
      leftRotation(node.leftChildren);
    }
    rightRotation(nodePointer);
  } else if (balance < -1) // Esta cargado a la derecha?
  {
    if (balancingFactor(node.rightChildren) >= 1) {
      rightRotation(node.rightChildren);
    }
    leftRotation(nodePointer);
  }
  return;
}

//*
template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::leftRotation(physical_pos nodePointer) const {
  AVLIndexNode<KEY_TYPE> a, b;
  file.seekg(nodePointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&a), sizeof(AVLIndexNode<KEY_TYPE>));

  physical_pos childPointer = a.rightChildren;

  file.seekg(a.rightChildren, std::ios::beg);
  file.read(reinterpret_cast<char *>(&b), sizeof(AVLIndexNode<KEY_TYPE>));

  a.rightChildren = b.leftChildren;
  b.leftChildren = childPointer;

  file.seekp(nodePointer, std::ios::beg);
  b.current_pos = nodePointer;
  file.write(reinterpret_cast<char *>(&b), sizeof(AVLIndexNode<KEY_TYPE>));

  file.seekp(childPointer, std::ios::beg);
  a.current_pos = childPointer;
  file.write(reinterpret_cast<char *>(&a), sizeof(AVLIndexNode<KEY_TYPE>));

  updateHeigth(childPointer);
  updateHeigth(nodePointer);

  return;
}

//*
template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::rightRotation(physical_pos nodePointer) const {
  AVLIndexNode<KEY_TYPE> a, b;
  file.seekg(nodePointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&a), sizeof(AVLIndexNode<KEY_TYPE>));

  physical_pos childPointer = a.leftChildren;

  file.seekg(a.leftChildren, std::ios::beg);
  file.read(reinterpret_cast<char *>(&b), sizeof(AVLIndexNode<KEY_TYPE>));

  a.leftChildren = b.rightChildren;
  b.rightChildren = childPointer;

  file.seekp(nodePointer, std::ios::beg);
  b.current_pos = nodePointer;
  file.write(reinterpret_cast<char *>(&b), sizeof(AVLIndexNode<KEY_TYPE>));

  file.seekp(childPointer, std::ios::beg);
  a.current_pos = childPointer;
  file.write(reinterpret_cast<char *>(&a), sizeof(AVLIndexNode<KEY_TYPE>));

  updateHeigth(childPointer);
  updateHeigth(nodePointer);
  return;
}

//* IS BALANCED
template <typename KEY_TYPE>
bool AVLIndex<KEY_TYPE>::isBalanced(physical_pos nodePointer) const {
  if (nodePointer == -1) {
    return true;
  }
  if (std::abs(balancingFactor(nodePointer)) > 1) {
    return false;
  }
  return true;
}

//* BALANCING FACTOR
template <typename KEY_TYPE>
int AVLIndex<KEY_TYPE>::balancingFactor(physical_pos nodePointer) const {
  if (nodePointer == -1) {
    return 0;
  }
  AVLIndexNode<KEY_TYPE> node;
  file.seekg(nodePointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&node), sizeof(AVLIndexNode<KEY_TYPE>));
  return height(node.leftChildren) - height(node.rightChildren);
}

//* UPDATE-HEIGHT FUNCTION
template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::updateHeigth(physical_pos nodePointer) const {
  if (nodePointer == -1) {
    return;
  }
  AVLIndexNode<KEY_TYPE> node;
  file.seekg(nodePointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&node), sizeof(AVLIndexNode<KEY_TYPE>));
  physical_pos hLeft = height(node.leftChildren);
  physical_pos hRigth = height(node.rightChildren);
  node.height = 1 + (hRigth > hLeft ? hRigth : hLeft);
  file.seekp(nodePointer, std::ios::beg);
  node.current_pos = nodePointer;
  file.write(reinterpret_cast<char *>(&node), sizeof(AVLIndexNode<KEY_TYPE>));
  return;
}

//* HEIGHT FUNCTION
template <typename KEY_TYPE>
long AVLIndex<KEY_TYPE>::height(physical_pos nodePointer) const {
  if (nodePointer == -1) {
    return -1;
  }
  AVLIndexNode<KEY_TYPE> node;
  file.seekg(nodePointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&node), sizeof(AVLIndexNode<KEY_TYPE>));
  return node.height;
}

//* SEARCH FUNCTION
template <typename KEY_TYPE>
AVLIndexNode<KEY_TYPE> AVLIndex<KEY_TYPE>::search(physical_pos currentPointer,
                                                  AVLIndexNode<KEY_TYPE> &cNode,
                                                  Data<KEY_TYPE> &item) const {
  if (currentPointer == -1) {
    throw std::runtime_error("No se ha encontrado el elemento!");
  }

  file.seekg(currentPointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));
  if (item > cNode.data) {
    return search(cNode.rightChildren, cNode, item);
  } else if (item < cNode.data) {
    return search(cNode.leftChildren, cNode, item);
  } else {
    return cNode;
  }
}

//*
template <typename KEY_TYPE>
physical_pos AVLIndex<KEY_TYPE>::maxNode(physical_pos nodePointer) const {
  if (nodePointer == -1) {
    throw std::runtime_error("El arbol está vacío!");
  }

  AVLIndexNode<KEY_TYPE> node;
  file.seekg(nodePointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&node), sizeof(AVLIndexNode<KEY_TYPE>));

  if (node.rightChildren == -1) {
    return nodePointer;
  } else {
    return maxNode(node.rightChildren);
  }
}

//*
template <typename KEY_TYPE>
bool AVLIndex<KEY_TYPE>::erase(physical_pos cPointer, physical_pos pPointer,
                               AVLIndexNode<KEY_TYPE> &cNode,
                               Data<KEY_TYPE> item, Response &response) const {
  if (cPointer == -1) {
    return false;
  }

  file.seekg(cPointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));

  if (item > cNode.data) {
    pPointer = cPointer;
    if (!erase(cNode.rightChildren, pPointer, cNode, item, response)) {
      return false;
    }
  } else if (item < cNode.data) {
    pPointer = cPointer;
    if (!erase(cNode.leftChildren, pPointer, cNode, item, response)) {
      return false;
    }
  } else {
    if (cNode.leftChildren == -1 && cNode.rightChildren == -1) {
      if (pPointer != -1) {
        AVLIndexNode<KEY_TYPE> pNode;
        file.seekg(pPointer, std::ios::beg);
        file.read(reinterpret_cast<char *>(&pNode),
                  sizeof(AVLIndexNode<KEY_TYPE>));

        if (pNode.leftChildren == cPointer) {
          pNode.leftChildren = -1;
        } else if (pNode.rightChildren == cPointer) {
          pNode.rightChildren = -1;
        }

        file.seekp(pPointer, std::ios::beg);
        pNode.current_pos = pPointer;
        file.write(reinterpret_cast<char *>(&pNode),
                   sizeof(AVLIndexNode<KEY_TYPE>));
      }

      // response.records.push_back(cPointer);
      cNode.nextDelete = header.lastDelete;
      file.seekp(cPointer, std::ios::beg);
      cNode.current_pos = cPointer;
      file.write(reinterpret_cast<char *>(&cNode),
                 sizeof(AVLIndexNode<KEY_TYPE>));

      header.lastDelete = cPointer;
      file.seekp(0, std::ios::beg);
      file.write(reinterpret_cast<char *>(&header), sizeof(AVLIndexHeader));
    } else if (cNode.leftChildren == -1) {
      if (pPointer != -1) {
        AVLIndexNode<KEY_TYPE> pNode;
        file.seekg(pPointer, std::ios::beg);
        file.read(reinterpret_cast<char *>(&pNode),
                  sizeof(AVLIndexNode<KEY_TYPE>));

        if (pNode.leftChildren == cPointer) {
          pNode.leftChildren = cNode.rightChildren;
        } else if (pNode.rightChildren == cPointer) {
          pNode.rightChildren = cNode.rightChildren;
        }

        file.seekp(pPointer, std::ios::beg);
        pNode.current_pos = pPointer;
        file.write(reinterpret_cast<char *>(&pNode),
                   sizeof(AVLIndexNode<KEY_TYPE>));
      }

      // response.records.push_back(cPointer);
      cNode.nextDelete = header.lastDelete;
      file.seekp(cPointer, std::ios::beg);
      cNode.current_pos = cPointer;
      file.write(reinterpret_cast<char *>(&cNode),
                 sizeof(AVLIndexNode<KEY_TYPE>));

      header.lastDelete = cPointer;
      file.seekp(0, std::ios::beg);
      file.write(reinterpret_cast<char *>(&header), sizeof(AVLIndexHeader));
    } else if (cNode.rightChildren == -1) {
      if (pPointer != -1) {
        AVLIndexNode<KEY_TYPE> pNode;
        file.seekg(pPointer, std::ios::beg);
        file.read(reinterpret_cast<char *>(&pNode),
                  sizeof(AVLIndexNode<KEY_TYPE>));

        if (pNode.leftChildren == cPointer) {
          pNode.leftChildren = cNode.leftChildren;
        } else if (pNode.rightChildren == cPointer) {
          pNode.rightChildren = cNode.leftChildren;
        }

        file.seekp(pPointer, std::ios::beg);
        pNode.current_pos = pPointer;
        file.write(reinterpret_cast<char *>(&pNode),
                   sizeof(AVLIndexNode<KEY_TYPE>));
      }

      //  response.records.push_back(cPointer);
      cNode.nextDelete = header.lastDelete;
      file.seekp(cPointer, std::ios::beg);
      cNode.current_pos = cPointer;
      file.write(reinterpret_cast<char *>(&cNode),
                 sizeof(AVLIndexNode<KEY_TYPE>));

      header.lastDelete = cPointer;
      file.seekp(0, std::ios::beg);
      file.write(reinterpret_cast<char *>(&header), sizeof(AVLIndexHeader));
    } else {
      physical_pos newPos = maxNode(cNode.leftChildren);
      AVLIndexNode<KEY_TYPE> tempNode;

      file.seekg(newPos, std::ios::beg);
      file.read(reinterpret_cast<char *>(&tempNode),
                sizeof(AVLIndexNode<KEY_TYPE>));

      //   response.records.push_back(cPointer);
      AVLIndexNode<KEY_TYPE> otherNode;
      Response fake;
      erase(header.rootPointer, -1, otherNode, tempNode.data, fake);
      fixValue(cPointer, otherNode, cNode.data, tempNode);
    }
  }

  updateHeigth(cPointer);
  if (!isBalanced(cPointer)) {
    balance(cPointer);
  }
  return true;
};

//*
template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::fixValue(physical_pos cPointer,
                                  AVLIndexNode<KEY_TYPE> &cNode,
                                  Data<KEY_TYPE> &item1,
                                  AVLIndexNode<KEY_TYPE> &tempNode) const {
  if (cPointer == -1) {
    return;
  }

  file.seekg(cPointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));

  if (cNode.data > item1) {
    return fixValue(cNode.leftChildren, cNode, item1, tempNode);
  } else if (cNode.data < item1) {
    return fixValue(cNode.rightChildren, cNode, item1, tempNode);
  } else {
    cNode.data = tempNode.data;
    cNode.raw_pos = tempNode.raw_pos;
    cNode.dup_pos = tempNode.dup_pos;
    file.seekp(cPointer, std::ios::beg);
    cNode.current_pos = cPointer;
    file.write(reinterpret_cast<char *>(&cNode),
               sizeof(AVLIndexNode<KEY_TYPE>));
  }
  return;
};

//*
template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::displayPretty(const std::string &prefix,
                                       physical_pos cPointer,
                                       bool isLeft) const {
  if (cPointer == -1) {
    return;
  }

  AVLIndexNode<KEY_TYPE> cNode;

  file.seekg(cPointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));

  std::cout << prefix;
  std::cout << (isLeft ? "|--" : "|__");
  std::cout << cNode.data.key << "(" << cNode.height << ")" << std::endl;

  displayPretty(prefix + (isLeft ? "|   " : "    "), cNode.leftChildren, true);
  displayPretty(prefix + (isLeft ? "|   " : "    "), cNode.rightChildren,
                false);
  return;
};

//*
template <typename KEY_TYPE>
void AVLIndex<KEY_TYPE>::rangeSearch(physical_pos cPointer,
                                     AVLIndexNode<KEY_TYPE> &cNode,
                                     Response &response, Data<KEY_TYPE> &begin,
                                     Data<KEY_TYPE> &end) const {
  if (cPointer == -1) {
    return;
  }

  file.seekg(cPointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));

  if (cNode.data > begin) {
    rangeSearch(cNode.leftChildren, cNode, response, begin, end);
    file.seekg(cPointer, std::ios::beg);
    file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));
  }
  if (cNode.data >= begin && cNode.data <= end) {
    this->getAllRawCurrentRecords(cNode, response.records);
    //    response.records.push_back(cNode.raw_pos);
  }
  if (cNode.data < end) {
    rangeSearch(cNode.rightChildren, cNode, response, begin, end);
    file.seekg(cPointer, std::ios::beg);
    file.read(reinterpret_cast<char *>(&cNode), sizeof(AVLIndexNode<KEY_TYPE>));
  }

  return;
};

//* ADD OPERATION
template <typename KEY_TYPE>
Response AVLIndex<KEY_TYPE>::add(Data<KEY_TYPE> data,
                                 physical_pos raw_pos) const {
  Response response;
  file.open(this->indexFileName,
            std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("No se pudo abrir el archivo AVLIndex!");
  }

  response.startTimer();

  try {
    AVLIndexNode<KEY_TYPE> insertNode;
    insertNode.data = data;
    insertNode.raw_pos = raw_pos;
    AVLIndexNode<KEY_TYPE> currentNode;

    insert(header.rootPointer, currentNode, insertNode, response);

    response.records.push_back(raw_pos);
  } catch (std::runtime_error) {
    response.stopTimer();
    throw std::runtime_error("Couldn't add");
  }

  response.stopTimer();
  file.close();
  return response;
};

//* SEARCH OPERATION
template <typename KEY_TYPE>
Response AVLIndex<KEY_TYPE>::search(Data<KEY_TYPE> item) const {
  Response response;
  file.open(this->indexFileName,
            std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("No se pudo abrir el archivo AVLIndex!");
  }
  response.startTimer();

  try {
    // Comentario
    AVLIndexNode<KEY_TYPE> searchNode;

    search(header.rootPointer, searchNode, item);
    // TODO: MODIFICAR
    this->getAllRawCurrentRecords(searchNode, response.records);
    // response.records.push_back(searchNode.raw_pos);
  } catch (std::runtime_error) {
    response.stopTimer();
    throw std::runtime_error("Couldn't search");
  }

  response.stopTimer();
  file.close();
  return response;
};

//* ERASE
template <typename KEY_TYPE>
Response AVLIndex<KEY_TYPE>::remove(Data<KEY_TYPE> item) const {
  Response response;
  file.open(this->indexFileName,
            std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("No se pudo abrir el archivo AVLIndex!");
  }
  response.startTimer();

  try {
    AVLIndexNode<KEY_TYPE> currentNode;

    [[maybe_unused]] bool isRemoved =
        erase(header.rootPointer, -1, currentNode, item, response);

    response.records.push_back(currentNode.raw_pos);
  } catch (std::runtime_error) {
    response.stopTimer();
    throw std::runtime_error("Couldn't erase");
  }

  response.stopTimer();
  file.close();
  return response;
};

//*
template <typename KEY_TYPE>
Response AVLIndex<KEY_TYPE>::range_search(Data<KEY_TYPE> start,
                                          Data<KEY_TYPE> end) const {
  Response response;
  file.open(this->indexFileName,
            std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("No se pudo abrir el archivo AVLIndex!");
  }
  response.startTimer();

  try {
    AVLIndexNode<KEY_TYPE> node;

    rangeSearch(header.rootPointer, node, response, start, end);
  } catch (std::runtime_error) {
    throw std::runtime_error("Couldn't range search");
  }

  response.stopTimer();
  file.close();
  return response;
};

template <typename KEY_TYPE> void AVLIndex<KEY_TYPE>::displayPretty() const {
  file.open(this->indexFileName,
            std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("No se pudo abrir el archivo AVLIndex!");
  }

  AVLIndexNode<KEY_TYPE> node;
  file.seekg(header.rootPointer, std::ios::beg);
  file.read(reinterpret_cast<char *>(&node), sizeof(AVLIndexNode<KEY_TYPE>));
  displayPretty("", header.rootPointer, true);
  std::cout << std::endl;

  file.close();
};

template <typename KEY_TYPE>
std::pair<Response, std::vector<bool>> AVLIndex<KEY_TYPE>::bulk_insert(
    const std::vector<std::pair<Data<KEY_TYPE>, physical_pos>> &records) const {
  Response response;
  response.startTimer();

  try {

    if (records.empty()) {
      response.stopTimer();
      return {response, {}};
    }

    file.open(this->indexFileName,
              std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
      throw std::runtime_error("No se pudo abrir el archivo AVLIndex!");
    }

    auto record_copy = records;

    for (auto &record : record_copy) {
      AVLIndexNode<KEY_TYPE> insertNode;
      insertNode.data = record.first;
      insertNode.raw_pos = record.second;
      AVLIndexNode<KEY_TYPE> currentNode;

      insert(header.rootPointer, currentNode, insertNode, response);
    }

    file.close();

  } catch (...) {
    file.close();
    response.stopTimer();
    throw std::runtime_error("Couldn't load records");
  }
  response.stopTimer();
  return {response, {}};
};

template <typename KEY_TYPE>
std::string AVLIndex<KEY_TYPE>::get_index_name() const {
  return this->index_name;
}

#endif // AVL_INDEX_CPP
