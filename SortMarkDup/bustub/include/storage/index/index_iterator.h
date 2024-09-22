//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

// index_iterator 并不支持并发访问
INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *buffer, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_page, int index);
  ~IndexIterator();

  bool isEnd() const;

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    if (isEnd() && itr.isEnd()) {
      return true;
    }
    if (isEnd() || itr.isEnd()) {
      return false;
    }
    if (buffer_pool_manager_ == itr.buffer_pool_manager_ && leaf_page_ == itr.leaf_page_ && index_ == itr.index_) {
      return true;
    }
    return false;
  }

  bool operator!=(const IndexIterator &itr) const { return !((*this) == itr); }

 private:
  BufferPoolManager *buffer_pool_manager_;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_page_;
  int index_;
};

}  // namespace bustub
