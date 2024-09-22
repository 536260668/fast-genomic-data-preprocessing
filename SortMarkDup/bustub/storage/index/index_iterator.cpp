/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
//INDEXITERATOR_TYPE::IndexIterator() = default;
INDEXITERATOR_TYPE::IndexIterator():buffer_pool_manager_(nullptr), leaf_page_(nullptr){
  std::cout << "default constructor\n";
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer,
                                  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_page, int index)
    : buffer_pool_manager_(buffer), leaf_page_(leaf_page), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page_ != nullptr) {
    // for operator++ return const, no change make to the page, not dirty.
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const {
  return (leaf_page_ == nullptr) || (index_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (isEnd()) {
    throw std::runtime_error("dereference on invalid iterator");
  }
  return leaf_page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    return *this;
  }
  if (index_ < leaf_page_->GetSize() - 1) {
    index_++;
    return *this;
  }
  if (leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    auto page_id = leaf_page_->GetNextPageId();
    // same reason as above, not dirty
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    auto tmp = buffer_pool_manager_->FetchPage(page_id);
    leaf_page_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(tmp->GetData());
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

// added by lhh
template class IndexIterator<GenericKey<8>, void*, GenericComparator<8>>;

template class IndexIterator<GenericKey<24>, RID, GenericComparator<24>>;


}  // namespace bustub
