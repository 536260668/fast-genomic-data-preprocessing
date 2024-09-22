/**
 * The implementation of BAMPartitioner class
 */

#include <cassert>
#include <filesystem>
#include "bam_partitioner.h"

 BAMPartitioner::BAMPartitioner(uint64_t max_ky, uint32_t np, uint64_t perp):
  num_partitions(np),
  range_size((max_ky + num_partitions - 1) / num_partitions),
  max_RDD_size_per_partition(perp){
  
  // set the buffer for the partitioned reads
  partitioned_Page_lock = new std::mutex[num_partitions];

  bam_buffer = new BAMRecordBuffer* [num_partitions];
  if (!std::filesystem::is_directory("temp") || !std::filesystem::exists("temp")) { // Check if temp folder exists
    std::filesystem::create_directory("temp"); // create temp folder
  }
  for(int i=0; i<num_partitions; i++)
  {
    bam_buffer[i] = new BAMRecordBuffer("./temp/tmp" + std::to_string(i) + ".db");
  }

  result = new tRDD[num_partitions];
  lk_result = new std::mutex[num_partitions];
}


uint32_t BAMPartitioner::selectPartition(BAMRecord* elem){
  return elem->partition_key() / range_size;
}

BAMPartitioner::~BAMPartitioner(){

  delete[] partitioned_Page_lock;
  for(int i=0; i<num_partitions; i++)
  {
    delete bam_buffer[i];
  }
  delete[] bam_buffer;

  delete[] lk_result;
  delete[] result;
  std::filesystem::remove_all("temp"); // remove temp folder
}

void BAMPartitioner::addElem(BAMRecord * elem, std::vector<tBuffer>* buffer){
  auto index = selectPartition(elem);

  partitioned_Page_lock[index].lock();
 
  size_t offset = bam_buffer[index]->addData(elem);


  partitioned_Page_lock[index].unlock();

  // add the offset in the file to the buffer
  (*buffer)[index].emplace_back(std::pair<uint64_t, size_t>(elem->sort_key(), offset));
  if((*buffer)[index].size() == buffer_size){
    buffer2RDD(buffer, index);
  }
  
  delete elem;
}


std::vector<BAMPartitioner::tRDD> BAMPartitioner::getResult()
{
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

void BAMPartitioner::destroyBuffer(std::vector<tBuffer>* buffer){
  std::lock_guard<std::mutex> lock(lk_bufBuffer);
  bufBuffer.push_back(buffer);
}

std::vector<typename BAMPartitioner::tBuffer>* BAMPartitioner::initBuffer(){
  std::unique_lock<std::mutex> lock(lk_bufBuffer);
  if(!bufBuffer.empty()){
    auto ret = bufBuffer.back();
    bufBuffer.pop_back();
    lock.unlock();
    return ret;
  }
  auto ret = new std::vector<BAMPartitioner::tBuffer>(num_partitions);
  for(auto &parti : *ret){
    parti.reserve(buffer_size);
  }
  return ret;
}

// 这么做，使用 buffer 究竟图什么呢？把细粒度的同步，换成粗粒度的，同步时等待的时间更长了，唯一的好处大概是减少进入内核态，获取 lock 的次数
// 但是一旦使用mmap，内存无限大，那么这里的锁就可以换成原子操作，意义就变成了减少 cache pingpong
void BAMPartitioner::buffer2RDD(std::vector<BAMPartitioner::tBuffer>* buffer, uint32_t index){
  std::lock_guard<std::mutex> lock(lk_result[index]);
  auto &tb = (*buffer)[index];
  auto &tr = result[index];
  for(auto &elem: tb){
    tr.push_back(elem);
  }
  tb.clear();
}

BAMRecordBuffer * BAMPartitioner::getBAMRecordBuffer(int i)
{
  return bam_buffer[i];
}
