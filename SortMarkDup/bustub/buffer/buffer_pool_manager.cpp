//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_id;
  // lock to search the page table for the requested page (p)
  std::unique_lock<std::mutex> lock(latch_);
  // Case 1: P exists in page table
  if (page_table_.count(page_id) > 0) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);

    // LOG_INFO("BufferPoolManager::FetchPage P exists in page table");
    return pages_ + frame_id;
  }

  // Case 2: find R from free list
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    SetHelper1(page_id, frame_id);
    // aquire frame latch before release lock, won't block
    pages_[frame_id].WLatch();
    lock.unlock();

    std::unique_lock<std::mutex> disk_lock(disk_latch_);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    disk_lock.unlock();
    pages_[frame_id].WUnlatch();

    // LOG_INFO("BufferPoolManager::FetchPage find R from free list");
    return pages_ + frame_id;
  }

  // Case 3: find R from replacer
  if (!replacer_->Victim(&frame_id)) {
    // LOG_INFO("BufferPoolManager::FetchPage can not find a replacement page from replacer");
    return nullptr;
  }

  auto &frame = pages_[frame_id];
  auto old_page_dirty = frame.IsDirty();
  auto old_page_id = frame.GetPageId();
  /**
   * Caution: Bug prone
   * Must write R back to disk first, then delete R from page table and insert P
   * Otherwise, 一个page M 被evict，但此时数据未被写出，这时另一个线程fetch M,
   * 发现M not exist, so it find a replacement page R, and fetch M from disk, but
   * the M on disk is out of date, or even not even exist.
   *
   */
  if (old_page_dirty) {
    frame.RLatch();
    std::unique_lock<std::mutex> disk_lock(disk_latch_);
    disk_manager_->WritePage(old_page_id, frame.GetData());
    disk_lock.unlock();
    frame.RUnlatch();
  }
  page_table_.erase(old_page_id);
  SetHelper1(page_id, frame_id);
  // aquire latch, won't block, for no other use it
  frame.WLatch();
  lock.unlock();

  std::unique_lock<std::mutex> disk_lock(disk_latch_);
  disk_manager_->ReadPage(page_id, frame.GetData());
  disk_lock.unlock();
  frame.WUnlatch();

  // LOG_INFO("BufferPoolManager::FetchPage find R from replacer");
  return pages_ + frame_id;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    // page_id not exist
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto &frame = pages_[frame_id];
  // return false if the page pin count is <= 0 before this call, true otherwise
  if (frame.pin_count_ <= 0) {
    return false;
  }
  frame.pin_count_--;
  // unpin page if it is not in use
  if (frame.pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  // if is_dirty is true, the page should be marked as dirty
  if (is_dirty) {
    frame.is_dirty_ = true;
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    // return false if the page could not be found in the page table
    return false;
  }
  auto frame_id = page_table_[page_id];
  // acquire RLatch before release lock, the special case is that user use Unpin(page_id = x, is_dirty = false)
  // but x has been modified. Or computer suffer poweroff
  pages_[frame_id].RLatch();
  lock.unlock();

  std::unique_lock<std::mutex> disk_lock(disk_latch_);
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  disk_lock.unlock();
  pages_[frame_id].RUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::unique_lock<std::mutex> disk_lock(disk_latch_);
  *page_id = disk_manager_->AllocatePage();
  disk_lock.unlock();
  frame_id_t frame_id;
  std::unique_lock<std::mutex> lock(latch_);

  // Case 1: pick a victim from free list
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    SetHelper1(*page_id, frame_id);
    // aquire frame latch before release lock, won't block
    pages_[frame_id].WLatch();
    lock.unlock();

    pages_[frame_id].ResetMemory();
    pages_[frame_id].WUnlatch();
    // LOG_INFO("BufferPoolManager::NewPage find R from free list");
    return pages_ + frame_id;
  }

  // Case 2: If all the pages in the buffer pool are pinned, return nullptr
  if (!replacer_->Victim(&frame_id)) {
    disk_lock.lock();
    disk_manager_->DeallocatePage(*page_id);
    disk_lock.unlock();
    std::cout << "all pages in the buffer pool are pinned" << std::endl;
    return nullptr;
  }

  // Case 3: pick a victim from replacer
  auto &frame = pages_[frame_id];
  auto old_page_dirty = frame.IsDirty();
  auto old_page_id = frame.GetPageId();
  if (old_page_dirty) {
    frame.RLatch();
    disk_lock.lock();
    disk_manager_->WritePage(old_page_id, frame.GetData());
    disk_lock.unlock();
    frame.RUnlatch();
  }
  page_table_.erase(old_page_id);
  SetHelper1(*page_id, frame_id);
  // aquire frame latch before release lock, won't block
  frame.WLatch();
  lock.unlock();

  frame.ResetMemory();
  frame.WUnlatch();
  // LOG_INFO("BufferPoolManager::NewPage find R from replacer");
  return pages_ + frame_id;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unique_lock<std::mutex> lock(latch_);
  // Case 1: If P does not exist, return true
  if (page_table_.count(page_id) == 0) {
    std::unique_lock<std::mutex> disk_lock(disk_latch_);
    disk_manager_->DeallocatePage(page_id);
    disk_lock.unlock();
    return true;
  }

  // Case 2: If P exists, but has a non-zero pin-count, return false
  auto frame_id = page_table_[page_id];
  auto &frame = pages_[frame_id];
  if (frame.GetPinCount() > 0) {
    return false;
  }

  // Case 3:  P can be deleted
  page_table_.erase(page_id);
  // reset its metadata
  frame.pin_count_ = 0;
  frame.page_id_ = INVALID_PAGE_ID;
  frame.is_dirty_ = false;
  // return it to the free list
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  lock.unlock();

  std::lock_guard<std::mutex> disk_lock(disk_latch_);
  disk_manager_->DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // May cause dead lock, maybe!!!
  std::unique_lock<std::mutex> lock(latch_);
  for (auto pair : page_table_) {
    auto frame_id = pair.second;
    auto page_id = pair.first;
    pages_[frame_id].RLatch();
    std::unique_lock<std::mutex> disk_lock(disk_latch_);
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    disk_lock.unlock();
    pages_[frame_id].RUnlatch();
  }
}

}  // namespace bustub
