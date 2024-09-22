//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  page_id_t GetNextPageId() const;
  void SetNextPageId(page_id_t next_page_id);
  KeyType KeyAt(int index) const;
  // Return pos, where KeyAt(pos) >= key and KeyAt(pos - 1) < key
  int KeyIndex(const KeyType &key, const KeyComparator &comparator) const;
  const MappingType &GetItem(int index) const;

  // insert and delete methods
  int Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);
  bool Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const;
  int RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator);

  // Split and Merge utility methods
  // Use case: when split, move half items to new right page
  void MoveHalfTo(BPlusTreeLeafPage *recipient);
  // Use case: when merge, move right page's items to left page
  void MoveAllTo(BPlusTreeLeafPage *recipient);
  // Use case: when redistribute, fetch first item of right page and append to left page's end
  void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);
  // Use case: when redistribute, fetch last item of left page and insert_front to right page
  void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

 private:
  void CopyNFrom(MappingType *items, int size);
  void CopyLastFrom(const MappingType &item);
  void CopyFirstFrom(const MappingType &item);
  page_id_t next_page_id_;
  MappingType array[LEAF_PAGE_SIZE];
};
}  // namespace bustub
