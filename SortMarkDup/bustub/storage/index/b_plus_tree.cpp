//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>  // NOLINT
#include <string>
#include <type_traits>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}
/*
 * Helper utility to travel from root to leaf in a latch crabbing way
 */
#define IS_INSERT_SAFE(page_data) ((page_data)->GetSize() < (page_data)->GetMaxSize())
#define IS_DELETE_SAFE(page_data) ((page_data)->GetSize() > (page_data)->GetMinSize())
#define QueueWUnlatchUnpin(transaction, buffer_pool_manager)               \
  {                                                                        \
    auto queue = (transaction)->GetPageSet();                              \
    while (!queue->empty()) {                                              \
      queue->back()->WUnlatch();                                           \
      (buffer_pool_manager)->UnpinPage(queue->back()->GetPageId(), false); \
      queue->pop_back();                                                   \
    }                                                                      \
  }
#define CLEAN_ROUTINE                                      \
  {                                                        \
    QueueWUnlatchUnpin(transaction, buffer_pool_manager_); \
    if (root_locked) {                                     \
      root_mutex_.unlock();                                \
      root_locked = false;                                 \
    }                                                      \
  }
/*
 * Latch crabbing travel down to leaf page, support search/insert/remove operation
 * @param     transaction        may hold the WLatched page
 * @param     root_locked        return value placement
 * @return    nullptr means b_plus_tree us empty, so nothing user need to do
 * If root_locked == true, user need `root_mutex_.unlock()`, otherwise do nothing.
 * User needs to Unlatch and Unpin the page returned
 */
INDEX_TEMPLATE_ARGUMENTS
template <OPERATION_TYPE OPERATION>
Page *BPLUSTREE_TYPE::LatchCrabbingToLeaf(const KeyType &key, Transaction *transaction, bool *root_locked) {
  // ---get root_page_id
  root_mutex_.lock();
  *root_locked = true;
  auto page_id = root_page_id_;
  if (page_id == INVALID_PAGE_ID) {
    // // if empty, clean up all, user needn't do anything
    // root_mutex_.unlock();
    // *root_locked = false;
    return nullptr;
  }
  auto page = buffer_pool_manager_->FetchPage(page_id);
  auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // ---fetch the root page and latch
  if (OPERATION == SEARCH_TRAVEL || OPERATION == REACH_LEFTMOST) {
    page->RLatch();
    root_mutex_.unlock();
    *root_locked = false;
  } else if (OPERATION == INSERT_TRAVEL || OPERATION == DELETE_TRAVEL) {
    page->WLatch();
  }
  // ---travel down to leaf page
  if (OPERATION == SEARCH_TRAVEL || OPERATION == REACH_LEFTMOST) {  // read
    while (!page_data->IsLeafPage()) {
      auto old_page_id = page_id;
      auto old_page = page;
      if (OPERATION == REACH_LEFTMOST) {
        page_id = (reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page))->ValueAt(0);
      } else {
        page_id = (reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page))
                      ->Lookup(key, comparator_);
      }
      page = buffer_pool_manager_->FetchPage(page_id);
      assert(page != nullptr);
      page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
      page->RLatch();
      old_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(old_page_id, false);
    }
  } else if (OPERATION == INSERT_TRAVEL || OPERATION == DELETE_TRAVEL) {  // modify
    while (!page_data->IsLeafPage()) {
      // Once child is locked, check if it is safe,
      bool safe_ = false;
      if (OPERATION == INSERT_TRAVEL) {
        if (IS_INSERT_SAFE(page_data)) {
          safe_ = true;
        }
      } else {
        if (IS_DELETE_SAFE(page_data)) {
          safe_ = true;
        }
      }
      if (safe_) {
        // release all locks on ancestors
        QueueWUnlatchUnpin(transaction, buffer_pool_manager_);
        if (*root_locked) {
          root_mutex_.unlock();
          *root_locked = false;
        }
      }
      // bookkeeping for later unlatch
      transaction->AddIntoPageSet(page);
      page_id = (reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page))
                    ->Lookup(key, comparator_);
      page = buffer_pool_manager_->FetchPage(page_id);
      page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
      page->WLatch();
    }
  }
  assert(page != nullptr);
  return page;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  // Case 1: none page exist
  root_mutex_.lock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_mutex_.unlock();
    return true;
  }
  // Case 2: page exist but empty
  bool ret = false;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->RLatch();
  root_mutex_.unlock();
  if (page_data->IsLeafPage() && page_data->GetSize() == 0) {
    ret = true;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return ret;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  bool root_locked;
  auto page = LatchCrabbingToLeaf<SEARCH_TRAVEL>(key, nullptr, &root_locked);
  if (page == nullptr) {
    if (root_locked) {
      root_mutex_.unlock();
      root_locked = false;
    }
    return false;
  }
  ValueType value;
  auto page_data = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  auto ret = page_data->Lookup(key, &value, comparator_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_data->GetPageId(), false);
  if (ret) {
    result->push_back(value);
  }
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id;
  // step 1: new page
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  auto page_data = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_page->GetData());
  // step 2: init
  page_data->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  // step 3: insert key-value pair
  page_data->Insert(key, value, comparator_);
  // step 4: tree adjust
  root_page_id_ = new_page_id;
  UpdateRootPageId(true);
  // step 5: unpin
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // step 1: travel to leaf page
  bool root_locked;
  auto leaf_page = LatchCrabbingToLeaf<INSERT_TRAVEL>(key, transaction, &root_locked);
  // step 2: elimite special case that return false
  // empty tree
  if (leaf_page == nullptr) {
    StartNewTree(key, value);
    if (root_locked) {
      root_mutex_.unlock();
      root_locked = false;
    }
    return true;
  }
  auto leaf_page_data = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  ValueType fake_value;
  // key exists already
  if (leaf_page_data->Lookup(key, &fake_value, comparator_)) {
    auto leaf_page_id = leaf_page_data->GetPageId();
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    CLEAN_ROUTINE;
    return false;
  }

  // step 3: insert and handle split
  leaf_page_data->Insert(key, value, comparator_);
  if (!IS_INSERT_SAFE(leaf_page_data)) {
    // case 2: leaf page full and handle split
    auto new_page_data = Split(leaf_page_data);
    // insert to parent
    leaf_page->WUnlatch();
    InsertIntoParent(leaf_page_data, new_page_data->KeyAt(0), new_page_data, transaction);
    buffer_pool_manager_->UnpinPage(new_page_data->GetPageId(), true);
  } else {
    // case 1: leaf page not full
    leaf_page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), true);
  CLEAN_ROUTINE;
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// N *BPLUSTREE_TYPE::Split(N *node){
//   page_id_t new_page_id;
//   // allocate and init new page
//   auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
//   if (new_page == nullptr) {
//     throw std::runtime_error("out of memory");
//   }
//   auto new_page_data = reinterpret_cast<N *>(new_page->GetData());
//   if(std::is_same<N, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>::value){
//     new_page_data->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
//     // set next page id
//     new_page_data->SetNextPageId(node->GetNextPageId());
//     node->SetNextPageId(new_page_id);
//     // handle split
//     node->MoveHalfTo(new_page_data);
//   }else{
//     new_page_data->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
//     // handle split
//     node->MoveHalfTo(new_page_data, buffer_pool_manager_);
//   }
//   return new_page_data;
// }

INDEX_TEMPLATE_ARGUMENTS
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *BPLUSTREE_TYPE::Split(
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *node) {
  page_id_t new_page_id;
  // allocate and init new page
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  auto new_page_data = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_page->GetData());
  new_page_data->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
  // set next page id
  new_page_data->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);
  // handle split
  node->MoveHalfTo(new_page_data);
  return new_page_data;
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *BPLUSTREE_TYPE::Split(
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *node) {
  page_id_t new_page_id;
  // allocate and init new page
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  auto new_page_data =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_page->GetData());
  new_page_data->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
  // handle split
  node->MoveHalfTo(new_page_data, buffer_pool_manager_);
  return new_page_data;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // the old node is the root page, case needs to update root page
  if (old_node->IsRootPage()) {
    page_id_t new_page_id;
    // step 1: new page
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (new_page == nullptr) {
      throw std::runtime_error("out of memory");
    }
    auto new_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_page);
    // step 2: init
    new_page_data->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    // step 3: data movement
    new_page_data->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // step 4: tree adjusment
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    // step 5: unpin and durability
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    UpdateRootPageId(false);
    return;
  }
  auto queue = transaction->GetPageSet();
  auto parent_page = queue->back();
  queue->pop_back();
  auto parent_page_data =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  auto parent_id = parent_page_data->GetPageId();
  // insert with no split
  if (IS_INSERT_SAFE(parent_page_data)) {
    parent_page_data->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    parent_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_page_data->GetPageId(), true);
    return;
  }
  // handler split case
  std::pair<KeyType, page_id_t> temp;  // buffer to hold the last element of new created page returned by Split
  // step 1: set temp correctly
  if (comparator_(key, parent_page_data->KeyAt(parent_page_data->GetSize() - 1)) < 0) {
    temp.first = parent_page_data->KeyAt(parent_page_data->GetSize() - 1);
    temp.second = parent_page_data->ValueAt(parent_page_data->GetSize() - 1);
    parent_page_data->IncreaseSize(-1);
    parent_page_data->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  } else {
    temp.first = key;
    temp.second = new_node->GetPageId();
  }
  // step 2: split
  auto uncle_page_data = Split(parent_page_data);
  // step 3: append temp to uncle's tail
  uncle_page_data->InsertNodeAfter(uncle_page_data->ValueAt(uncle_page_data->GetSize() - 1), temp.first, temp.second);
  // set the rightmost node's parent to uncle_page_id
  //   Assert(the rightmost node cann't be old_node)
  if (temp.second == new_node->GetPageId()) {
    new_node->SetParentPageId(uncle_page_data->GetPageId());
  } else {
    // see b_plus_tree_internal_page.cpp : REPLACE
    auto page_id = temp.second;
    auto page = buffer_pool_manager_->FetchPage(page_id);
    auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->WLatch();
    page_data->SetParentPageId(uncle_page_data->GetPageId());
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
  }

  // split recursively
  // 约定：internal page split后KeyAt(0) 存放的是需要pop up 的值
  parent_page->WUnlatch();
  InsertIntoParent(parent_page_data, uncle_page_data->KeyAt(0), uncle_page_data, transaction);
  buffer_pool_manager_->UnpinPage(uncle_page_data->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // delete record

  // adjust tree if necessary, ajust mean coleasce or redistribute
  // step 1: travel to leaf page
  bool root_locked;
  auto leaf_page = LatchCrabbingToLeaf<DELETE_TRAVEL>(key, transaction, &root_locked);
  // if tree is empty, return immdiately
  if (leaf_page == nullptr) {
    if (root_locked) {
      root_mutex_.unlock();
      root_locked = false;
    }
    return;
  }
  auto leaf_page_data = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  auto leaf_page_id = leaf_page_data->GetPageId();
  // delete and handle redistribute or merge if necessary
  // delete record
  auto page_size = leaf_page_data->RemoveAndDeleteRecord(key, comparator_);
  auto min_size = leaf_page_data->GetMinSize();
  // 这里应该是从底层反推对参数的要求
  if (page_size < min_size && CoalesceOrRedistribute(leaf_page_data, transaction)) {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    transaction->AddIntoDeletedPageSet(leaf_page_id);
    buffer_pool_manager_->DeletePage(leaf_page_id);
  } else {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  }
  // release transaction, root_mutex_
  CLEAN_ROUTINE;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *page_data, Transaction *transaction) {
  // decide to merge or redistribute, i.e., the operator, the operand
  //--- Special case: root page
  if (page_data->IsRootPage()) {
    // std::cout << "reset root page" << std::endl;
    return AdjustRoot(page_data);
  }

  auto queue = transaction->GetPageSet();
  if (queue->empty()) {
    std::cout << "page size " << page_data->GetSize() << std::endl;
    LOG_DEBUG("CR: queue is empty");
  }
  auto parent_page = queue->back();
  queue->pop_back();
  auto parent_page_data =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  auto parent_id = parent_page_data->GetPageId();
  auto page_index = parent_page_data->ValueIndex(page_data->GetPageId());

  //--- find sibling to redistrubute with
  auto redistribute_index = page_index;
  N *sibling_page_data = nullptr;
  Page *sibling_page = nullptr;
  page_id_t sibling_id;
  // test left sibling
  if (page_index > 0) {
    sibling_id = parent_page_data->ValueAt(page_index - 1);
    sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    sibling_page_data = reinterpret_cast<N *>(sibling_page->GetData());
    sibling_page->WLatch();
    if (sibling_page_data->GetSize() + page_data->GetSize() <= page_data->GetMaxSize()) {
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, false);
      sibling_page_data = nullptr;
    }
  }
  // test right sibling
  if (sibling_page_data == nullptr && page_index + 1 < parent_page_data->GetSize()) {
    sibling_id = parent_page_data->ValueAt(page_index + 1);
    sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    sibling_page_data = reinterpret_cast<N *>(sibling_page->GetData());
    sibling_page->WLatch();
    if (sibling_page_data->GetSize() + page_data->GetSize() <= page_data->GetMaxSize()) {
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, false);
      sibling_page_data = nullptr;
    } else {
      redistribute_index++;
      redistribute_index = -redistribute_index;
    }
  }

  //--- redistribute
  if (sibling_page_data != nullptr) {
    Redistribute(sibling_page_data, page_data, redistribute_index);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_id, true);
    parent_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_id, false);
    return false;
  }

  //--- coalesce
  bool ret;
  if (page_index == 0) {
    sibling_id = parent_page_data->ValueAt(page_index + 1);
    ret = false;
  } else {
    sibling_id = parent_page_data->ValueAt(page_index - 1);
    ret = true;
  }
  sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
  sibling_page_data = reinterpret_cast<N *>(sibling_page->GetData());
  sibling_page->WLatch();
  bool CR_recursively = Coalesce(&sibling_page_data, &page_data, &parent_page_data, page_index, transaction);
  bool parent_delete = false;
  // handle sibling
  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_id, true);
  if (page_index == 0) {
    transaction->AddIntoDeletedPageSet(sibling_id);
    buffer_pool_manager_->DeletePage(sibling_id);
  }
  if (CR_recursively) {
    auto page = buffer_pool_manager_->FetchPage(page_data->GetPageId());
    if (page->GetPinCount() < 2) {
      LOG_DEBUG("CR: pin count is one");
    }
    page->WUnlatch();
    parent_delete = CoalesceOrRedistribute(parent_page_data, transaction);
    if (page->GetPinCount() > 2) {
      LOG_DEBUG("CR: pin more than 2 times");
    }
    page->WLatch();
    buffer_pool_manager_->UnpinPage(page_data->GetPageId(), true);
  }
  // handle parent
  parent_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(parent_id, true);
  if (parent_delete) {
    transaction->AddIntoDeletedPageSet(parent_id);
    buffer_pool_manager_->DeletePage(parent_id);
  }
  return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              node_.GetPageId() == parent_.ValueAt(index)
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node_, N **node_,
//                               BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent_, int index,
//                               Transaction *transaction) { // done
//   auto neighbor_page_data = *neighbor_node_;
//   auto page_data = *node_;
//   auto parent_page_data = *parent_;

//   // step1: merge the right sibling's content to left
//   if(std::is_same<N, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>::value){
//     // merge and set recipient's next page id
//     page_data->MoveAllTo(neighbor_page_data);
//   }else{
//     if(index == 0){
//       page_data->MoveAllTo(neighbor_page_data, parent_page_data->KeyAt(1), buffer_pool_manager_);
//     }else{
//       page_data->MoveAllTo(neighbor_page_data, parent_page_data->KeyAt(index), buffer_pool_manager_);
//     }
//   }
//   // step2: delete record in parent node and adjust tree if necessary
//   parent_page_data->Remove(index);
//   if(parent_page_data->GetSize() < parent_page_data->GetMinSize()){
//     return CoalesceOrRedistribute(parent_page_data, transaction);
//   }
//   return false;
// }

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Coalesce(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> **neighbor_node_,
                              BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> **node_,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent_, int index,
                              Transaction *transaction) {  // done
  auto neighbor_page_data = *neighbor_node_;
  auto page_data = *node_;
  auto parent_page_data = *parent_;

  // step1: merge the right sibling's content to left
  if (index == 0) {
    // Speical case: page_data as left sibling
    neighbor_page_data->MoveAllTo(page_data);
  } else {
    page_data->MoveAllTo(neighbor_page_data);
  }

  // step2: delete record in parent node and adjust tree if necessary
  if (index == 0) {
    index = 1;
  }
  parent_page_data->Remove(index);
  return parent_page_data->GetSize() < parent_page_data->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Coalesce(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **neighbor_node_,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **node_,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent_, int index,
                              Transaction *transaction) {  // done
  auto neighbor_page_data = *neighbor_node_;
  auto page_data = *node_;
  auto parent_page_data = *parent_;

  // step1: merge the right sibling's content to left
  if (index == 0) {
    neighbor_page_data->MoveAllTo(page_data, parent_page_data->KeyAt(1), buffer_pool_manager_);
  } else {
    page_data->MoveAllTo(neighbor_page_data, parent_page_data->KeyAt(index), buffer_pool_manager_);
  }
  // step2: delete record in parent node and adjust tree if necessary
  if (index == 0) {
    index = 1;
  }
  parent_page_data->Remove(index);
  return parent_page_data->GetSize() < parent_page_data->GetMinSize();
}

/*
 * Redistribute key & value pairs from one page to its sibling page.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   index              indicate whether neighbor_node as node's left
 * sibling or right. if index > 0, neighbor_node->MoveFirstToEndOf(node),
 * otherwise neighbor_node->MoveLastToFrontOf(node)
 * abs(index) as parent_page_data->ValueAt(the right sibling of two)
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) { // done
//   // Get parent page
//   auto parent_id = node->GetParentPageId();
//   auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
//   auto parent_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
//   KeyComparator>*>(parent_page->GetData()); // modify parent without aquire latch again
//   /*
//    * different case to
//    * step 1: move record between two sibling
//    * step 2: update parent record
//    */
//   if(std::is_same<N, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>::value){
//     if(index == 0){
//       neighbor_node->MoveFirstToEndOf(node);
//       parent_page_data->SetKeyAt(1, neighbor_node->KeyAt(0));
//     }else{
//       neighbor_node->MoveLastToFrontOf(node);
//       parent_page_data->SetKeyAt(index, node->KeyAt(0));
//     }
//   }else{
//     if(index == 0){
//       auto tmp = neighbor_node->KeyAt(1);
//       neighbor_node->MoveFirstToEndOf(node, parent_page_data->KeyAt(1), buffer_pool_manager_);
//       parent_page_data->SetKeyAt(1, tmp);
//     }else{
//       auto tmp = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
//       neighbor_node->MoveLastToFrontOf(node, parent_page_data->KeyAt(index), buffer_pool_manager_);
//       parent_page_data->SetKeyAt(index, tmp);
//     }
//   }
//   buffer_pool_manager_->UnpinPage(parent_id, true);
// }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *neighbor_node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *node, int index) {
  // Get parent page
  auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  // modify parent without aquire latch again
  auto parent_page_data =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  /*
   * different case to
   * step 1: move record between two sibling
   * step 2: update parent record
   */
  if (index < 0) {
    auto tmp = neighbor_node->KeyAt(1);
    neighbor_node->MoveFirstToEndOf(node, parent_page_data->KeyAt(-index), buffer_pool_manager_);
    parent_page_data->SetKeyAt(-index, tmp);
  } else {
    auto tmp = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
    neighbor_node->MoveLastToFrontOf(node, parent_page_data->KeyAt(index), buffer_pool_manager_);
    parent_page_data->SetKeyAt(index, tmp);
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *neighbor_node,
                                  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *node, int index) {
  // Get parent page
  auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  // modify parent without aquire latch again
  auto parent_page_data =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  /*
   * different case to
   * step 1: move record between two sibling
   * step 2: update parent record
   */
  if (index < 0) {
    neighbor_node->MoveFirstToEndOf(node);
    parent_page_data->SetKeyAt(-index, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node);
    parent_page_data->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case 2
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
    header_page->WLatch();
    header_page->DeleteRecord(index_name_);
    header_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
    return true;
  }
  // case 1
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    root_page_id_ = (reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node))
                        ->RemoveAndReturnOnlyChild();
    auto page_id = root_page_id_;
    auto page = buffer_pool_manager_->FetchPage(page_id);
    auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->WLatch();
    page_data->SetParentPageId(INVALID_PAGE_ID);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
    UpdateRootPageId();
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType fake_key;
  auto page = FindLeafPage(fake_key, true);
  assert(page != nullptr);
  // if (page == nullptr) {
  //   return INDEXITERATOR_TYPE();
  // }
  page->RUnlatch();
  return INDEXITERATOR_TYPE(buffer_pool_manager_,
                            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto page = FindLeafPage(key, false);
  if (page == nullptr) {
    return INDEXITERATOR_TYPE();
  }
  page->RUnlatch();
  auto index =
      (reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page))->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_,
                            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page), index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
// Notion: after call this method, the caller should unpin the page and unlatch
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  bool fake_val;
  return leftMost ? LatchCrabbingToLeaf<REACH_LEFTMOST>(key, nullptr, &fake_val)
                  : LatchCrabbingToLeaf<SEARCH_TRAVEL>(key, nullptr, &fake_val);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto page_id = HEADER_PAGE_ID;
  auto page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(page_id));
  page->WLatch();
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    page->UpdateRecord(index_name_, root_page_id_);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
//   int64_t key;
//   std::ifstream input(file_name);
//   while (input) {
//     input >> key;

//     KeyType index_key;
//     index_key.SetFromInteger(key);
//     RID rid(key);
//     Insert(index_key, rid, transaction);
//   }
// }
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

template class BPlusTree<GenericKey<8>, void *, GenericComparator<8>>;
template class BPlusTree<GenericKey<24>, RID, GenericComparator<24>>;
}  // namespace bustub
