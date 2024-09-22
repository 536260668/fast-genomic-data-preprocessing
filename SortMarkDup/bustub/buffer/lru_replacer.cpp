//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (order_.empty()) {
    return false;
  }
  *frame_id = order_.back();
  order_.pop_back();
  hash_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (hash_.find(frame_id) != hash_.end()) {
    order_.erase(hash_[frame_id]);
    hash_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (hash_.find(frame_id) != hash_.end() || order_.size() == capacity_) {
    return;
  }
  order_.push_front(frame_id);
  hash_[frame_id] = order_.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(latch_);
  return order_.size();
}

}  // namespace bustub
