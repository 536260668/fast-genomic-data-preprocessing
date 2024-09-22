#ifndef RANGE_PARTITIONER_HH
#define RANGE_PARTITIONER_HH

#include <vector>
#include <mutex>
#include <iostream>
#include <cstdio>
#include <cstring>
#include "bam_record.h"
#include "pair.h"

// use example
// a thread's work
// void thread(RangePartitioner& rangepartitioner){
//   auto buffer = rangepartitioner.initBuffer();
//   while(some_condition){
//     auto elem = generate_elem();
//     rangepartitioner.addElem(buffer, elem);
//   }
//   rangepartitioner.destroyBuffer(buffer);
// }
// void main_thread(){
//   // spawn multithread do rangePartition for all RDD
//   // join all thread
//   auto result = getResult();
//   // 所以 rangePartition 不做同步工作，在外部进行整个流程的同步
// }

// RangePartitioner 存放的是记录(record)，而不是记录的指针(pointer). 中间的暂存 buffer 可以存放记录的指针
// class constraint:
//////必须等全部线程都调用了 destroyBuffer 才可以调用 getResult, 一旦调用了 getResult, RangePartitioner 所接受的唯一操作就是析构
////// IPartitionElem 必须有 partition_key 方法，且返回 uint64_t
template<typename IPartitionElem>
class RangePartitioner{
  // -----------------variant
public:
  // class rp_buffer_t{
  // private:
  //   rp_buffer_t();
  // public:
  //   ~rp_buffer_t();
  //   friend class RangePartitioner;

  // };
  // 供每个线程添加元素的暂存缓冲, 要支持 size, capacity, push_back 方法
  typedef std::vector<IPartitionElem*> tBuffer;
  // 分区器完成后，所有每个分区内容存放的位置
  typedef std::vector<IPartitionElem*> tRDD;
private:
  std::mutex* lk_result; // 保证对result 的某个 RDD 的互斥访问
  const uint32_t buffer_size = 500; // 暂存 buffer 的 capacity

  // -----------------static
public:
  // @ max_key   IPartitionElem 的 partition_key() 的上限
  RangePartitioner(uint64_t max_key, uint32_t num_partitions, uint64_t max_RDD_size_per_partition);
  ~RangePartitioner();
  // 为一个线程分配一个 buffer
  std::vector<RangePartitioner<IPartitionElem>::tBuffer>* initBuffer();
  // 向 RangePartitioner 添加一个元素，需要先经过 buffer 作为缓冲
  void addElem(std::vector<RangePartitioner<IPartitionElem>::tBuffer>* buffer, IPartitionElem* elem);
  // 回收 buffer。如果 buffer 中有内容，那么需要先把内容移到 RDD
  void destroyBuffer(std::vector<RangePartitioner<IPartitionElem>::tBuffer>* buffer);
  // 获得分区后的结果
  std::vector<RangePartitioner<IPartitionElem>::tRDD> getResult();
private:
  // 确定一个 elem 属于哪个分区
  uint32_t selectPartition(IPartitionElem* elem);
  // 把 buffer[index] 的内容移入到RDD
  void buffer2RDD(std::vector<RangePartitioner<IPartitionElem>::tBuffer>* buffer, uint32_t index);

  const uint32_t num_partitions; // partition 的个数
  const uint64_t range_size; // 每个partition 覆盖 partition key 的个数
  const uint64_t max_RDD_size_per_partition;
  tRDD *result; // 存取分区的结果

  std::vector<std::vector<RangePartitioner<IPartitionElem>::tBuffer>*> bufBuffer; // 用于避免频繁的 initBuffer, destroyBuffer 造成的内存的 allocate 和 deallocate
  std::mutex lk_bufBuffer;
};


template<typename IPartitionElem>
RangePartitioner<IPartitionElem>::RangePartitioner(uint64_t max_ky, uint32_t np, uint64_t perp):
  num_partitions(np),
  range_size((max_ky + num_partitions - 1) / num_partitions),
  max_RDD_size_per_partition(perp){
  result = new tRDD[num_partitions];  //--- the data structure needs to be changed from vector to b+ tree
  lk_result = new std::mutex[num_partitions];
}

template<typename IPartitionElem>
RangePartitioner<IPartitionElem>::~RangePartitioner(){
  delete[] lk_result;
  delete[] result;
}

template<typename IPartitionElem>
uint32_t RangePartitioner<IPartitionElem>::selectPartition(IPartitionElem* elem){
  return elem->partition_key() / range_size;
}

template<typename IPartitionElem>
void RangePartitioner<IPartitionElem>::addElem(std::vector<RangePartitioner<IPartitionElem>::tBuffer>* buffer, IPartitionElem* elem){
  auto index = selectPartition(elem);
  (*buffer)[index].push_back(elem);
  if((*buffer)[index].size() == buffer_size){
    buffer2RDD(buffer, index);
  }
}

template<typename IPartitionElem>
void RangePartitioner<IPartitionElem>::destroyBuffer(std::vector<RangePartitioner<IPartitionElem>::tBuffer>* buffer){
  std::lock_guard<std::mutex> lock(lk_bufBuffer);
  bufBuffer.push_back(buffer);
}


template<typename IPartitionElem>
std::vector<typename RangePartitioner<IPartitionElem>::tBuffer>* RangePartitioner<IPartitionElem>::initBuffer(){
  std::unique_lock<std::mutex> lock(lk_bufBuffer);
  if(!bufBuffer.empty()){
    auto ret = bufBuffer.back();
    bufBuffer.pop_back();
    lock.unlock();
    return ret;
  }
  auto ret = new std::vector<RangePartitioner<IPartitionElem>::tBuffer>(num_partitions);
  for(auto &parti : *ret){
    parti.reserve(buffer_size);
  }
  return ret;
}

template<typename IPartitionElem>
std::vector<typename RangePartitioner<IPartitionElem>::tRDD> RangePartitioner<IPartitionElem>::getResult(){
  std::lock_guard<std::mutex> lock(lk_bufBuffer);
  while(!bufBuffer.empty()){
    auto &buffer = bufBuffer.back();
    bufBuffer.pop_back();
    for(uint32_t i = 0; i < num_partitions; i++){
      if((*buffer)[i].size() > 0){
        buffer2RDD(buffer,i);
      }
    }
    delete buffer;
  }
  std::vector<tRDD> ret(num_partitions);
  for(uint32_t i = 0; i < num_partitions; i++){
    ret[i] = std::move(result[i]);
  }
  return ret;
}

// 这么做，使用 buffer 究竟图什么呢？把细粒度的同步，换成粗粒度的，同步时等待的时间更长了，唯一的好处大概是减少进入内核态，获取 lock 的次数
// 但是一旦使用mmap，内存无限大，那么这里的锁就可以换成原子操作，意义就变成了减少 cache pingpong
template<typename IPartitionElem>
void RangePartitioner<IPartitionElem>::buffer2RDD(std::vector<RangePartitioner<IPartitionElem>::tBuffer>* buffer, uint32_t index){
  std::lock_guard<std::mutex> lock(lk_result[index]);
  auto &tb = (*buffer)[index];
  auto &tr = result[index];
  for(auto &elem: tb){
    tr.push_back(elem);
  }
  tb.clear();
}


#endif
