/*
 * The class for Bam_partitioner, using buffer pool and b+ tree to store the data
 * author: lhh
 */

#ifndef BAM_PARTITIONER_H
#define BAM_PARTITIONER_H

#include <vector>
#include <mutex>
#include <iostream>
#include <cstdio>
#include <cstring>
#include "bam_record.h"
#include "BAMRecordBuffer.h"

class BAMPartitioner{
public:
    const static int RecordSize = sizeof(BAMRecord);

    BAMPartitioner(uint64_t max_ky, uint32_t np, uint64_t perp);
    ~BAMPartitioner();
    

    // 供每个线程添加元素的暂存缓冲, 要支持 size, capacity, push_back 方法
    typedef std::vector<std::pair<uint64_t, size_t>> tBuffer;
    // 分区器完成后，所有每个分区内容存放的位置
    typedef std::vector<std::pair<uint64_t, size_t>> tRDD;

    // 为一个线程分配一个 buffer
    std::vector<tBuffer>* initBuffer();

    // 回收 buffer。如果 buffer 中有内容，那么需要先把内容移到 RDD
    void destroyBuffer(std::vector<tBuffer>* buffer);

    // 获得分区后的结果
    std::vector<tRDD> getResult();

    // 把 buffer[index] 的内容移入到RDD
    void buffer2RDD(std::vector<tBuffer>* buffer, uint32_t index);

    void addElem(BAMRecord* elem, std::vector<tBuffer>* buffer);  

    // get the data bufffer
    BAMRecordBuffer * getBAMRecordBuffer(int i);

private:
    const uint32_t num_partitions; // partition 的个数
    const uint64_t range_size; // 每个partition 覆盖 partition key 的个数
    const uint64_t max_RDD_size_per_partition;

    BAMRecordBuffer ** bam_buffer;

    std::mutex * partitioned_Page_lock;

    std::mutex* lk_result; // 保证对result 的某个 RDD 的互斥访问
    const uint32_t buffer_size = 500; // 暂存 buffer 的 capacity

    tRDD *result; // 存取分区的结果

    //---Is this member useful now?
    std::vector<std::vector<tBuffer>*> bufBuffer; // 用于避免频繁的 initBuffer, destroyBuffer 造成的内存的 allocate 和 deallocate
    std::mutex lk_bufBuffer;


    // 确定一个 elem 属于哪个分区
    uint32_t selectPartition(BAMRecord* elem);

};


#endif