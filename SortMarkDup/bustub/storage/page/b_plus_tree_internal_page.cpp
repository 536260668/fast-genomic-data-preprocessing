//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(bustub::IndexPageType::INTERNAL_PAGE);
  SetLSN();
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // keyAt(0) 可以用来存储split 时候的分界值
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
// return -1 as not find
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

template <typename KeyType, typename KeyComparator>
static bool less(const KeyComparator &comp, const KeyType &a, const KeyType &b) {
  return comp(a, b) == -1;
}

template <typename KeyType, typename KeyComparator>
static bool equal(const KeyComparator &comp, const KeyType &a, const KeyType &b) {
  return comp(a, b) == 0;
}

template <typename KeyType, typename KeyComparator>
static bool great(const KeyComparator &comp, const KeyType &a, const KeyType &b) {
  return comp(a, b) == 1;
}

template <typename KeyType, typename KeyComparator>
static bool less_equal(const KeyComparator &comp, const KeyType &a, const KeyType &b) {
  return comp(b, a) < 1;
}

template <typename KeyType, typename KeyComparator>
static bool great_equal(const KeyComparator &comp, const KeyType &a, const KeyType &b) {
  return comp(a, b) > -1;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
// A left pointer in an internal node guides towards keys < than its corresponding key,
// while a right pointer guides towards keys >=
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int begin = 0;
  int end = GetSize() - 1;
  if (GetSize() == 0) {
    LOG_DEBUG("InternalPage::LookUp page size is 0");
  }
  // loop invariant: KeyAt(begin) < key
  while (begin != end) {
    auto middle = (begin + end + 1) / 2;
    if (less(comparator, KeyAt(middle), key)) {
      begin = middle;
    } else if (equal(comparator, KeyAt(middle), key)) {
      begin = middle;
      end = middle;
    } else {
      end = middle - 1;
    }
  }
  return ValueAt(end);
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  IncreaseSize(2);
  SetKeyAt(1, new_key);
  array[0].second = old_value;
  array[1].second = new_value;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  auto index = ValueIndex(old_value);
  assert(index != -1);
  IncreaseSize(1);
  int i = GetSize() - 1;
  while (i - 1 != index) {
    array[i] = array[i - 1];
    i--;
  }
  array[i].second = new_value;
  SetKeyAt(i, new_key);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int index = GetMinSize();
  recipient->CopyNFrom(array + index, GetSize() - index, buffer_pool_manager);
  recipient->IncreaseSize(GetSize() - index);
  SetSize(index);
}

#define REPLACE(page_id, expre, buffer_pool_manager, is_dirty)           \
  {                                                                      \
    auto page = (buffer_pool_manager)->FetchPage(page_id);               \
    auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData()); \
    page->WLatch();                                                      \
    page_data->expre;                                                    \
    page->WUnlatch();                                                    \
    (buffer_pool_manager)->UnpinPage(page_id, is_dirty);                 \
  }

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size; i++) {
    // copy
    array[i] = items[i];
    // adopt
    auto page_id = array[i].second;
    REPLACE(page_id, SetParentPageId(GetPageId()), buffer_pool_manager, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto ret = ValueAt(0);
  SetSize(0);
  return ret;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  auto this_size = GetSize();
  auto recipient_old_size = recipient->GetSize();
  auto recipient_id = recipient->GetPageId();
  recipient->IncreaseSize(this_size);

  for (int i = 0; i < this_size; i++) {
    // move key-value pair
    recipient->array[recipient_old_size + i] = array[i];
    // reset child's parent_id
    auto page_id = ValueAt(i);
    REPLACE(page_id, SetParentPageId(recipient_id), buffer_pool_manager, true);
  }
  // add middle key to maintain the invariant
  recipient->SetKeyAt(recipient_old_size, middle_key);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // append key-value pair to recipient
  recipient->IncreaseSize(1);
  recipient->SetKeyAt(recipient->GetSize() - 1, middle_key);
  recipient->array[recipient->GetSize() - 1].second = ValueAt(0);
  // reset child's parent_id
  auto page_id = ValueAt(0);
  REPLACE(page_id, SetParentPageId(recipient->GetPageId()), buffer_pool_manager, true);
  // shift left
  for (int i = 0; i < GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // 功能在 MoveFirstToEndOf 中做了
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->IncreaseSize(1);
  // shift recipient right
  for (int i = recipient->GetSize() - 1; i > 0; i--) {
    recipient->array[i] = recipient->array[i - 1];
  }
  // insert key-value pair to recipient
  recipient->SetKeyAt(1, middle_key);
  recipient->array[0].second = ValueAt(GetSize() - 1);
  // reset child's parent_id
  auto page_id = recipient->ValueAt(0);
  REPLACE(page_id, SetParentPageId(recipient->GetPageId()), buffer_pool_manager, true);
  // decrease size
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // 功能在上面的函数中做了
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

template class BPlusTreeInternalPage<GenericKey<24>, page_id_t, GenericComparator<24>>;
}  // namespace bustub
