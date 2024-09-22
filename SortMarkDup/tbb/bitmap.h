#ifndef _BITMAP_HH
#define _BITMAP_HH

#include <atomic>

// bitmap 一经创建长度将不会变动，创建时所有位置 0
// bitmap 是线程安全的，但要求所有 set 都调用完成后方可调用 get 方法
class bitmap{
public:
  bitmap(uint64_t size);
  ~bitmap();
  // set bitmap[pos] = 1
  void set(uint64_t pos);
  // get bitmap[pos]
  bool get(uint64_t pos);
private:
  std::atomic_uint64_t *array;
  uint64_t size;
};
// 这个东西非常容易实现，我现在只是突然困惑，硬件如何保证 fetch_or 操作的原子性

#endif
