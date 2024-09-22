#include "tbb/range_partitioner.h"
#include "tbb/bam_partitioner.h"
#include <tbb/tbb.h>
#include <tbb/parallel_for_each.h>
#include <tbb/parallel_for.h>
#include <cassert>
#include <filesystem>
#include "tbb/bam_record.h"
#include "tbb/pair.h"
#include "tbb/bitmap.h"
#include "tbb/SinglePairCache.h"
#include "tbb/DoublePairCache.h"
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <iostream>
#include <algorithm>
#include <chrono>
#include <string>
#include "bgzf.h"
#include "hfile.h"
#include "kseq.h"
#include "tbb/concurrentqueue.h"
#include "tbb/bam_parser.h"
#include "getopt.h"
#include "tbb/SAMRead.h"

#define BULK_SIZE 10000
#define INT_SIZE sizeof(int)

namespace fs = std::filesystem;

// 分配pairID相关
static std::atomic_uint64_t pairIDSource = 1;
static const  uint32_t pairIDASC = 100;

void time_stamp(std::string hint);
char *auto_index(htsFile *fp, const char *fn, bam_hdr_t *header);
void read_alignment(htsFile *fp, sam_hdr_t * header);
void output_alignment(char* output_file, const sam_hdr_t *header, std::vector<BAMPartitioner::tRDD>& rdds, int num_partitions, BAMPartitioner& bam_partitioner
    , bitmap& duplicate_index);

moodycamel::ConcurrentQueue<std::vector<kstring_t> *> LineQueue(1000);
std::atomic_bool read_finished;

// usage: ./sormadup [-I input.sam] [-t num] -O output.bam
int main(int argc, char* argv[]){
    // by default, use one thread to read and the left to process
    int num_thread_shuffle = std::thread::hardware_concurrency() - 1;
    int c;
    char * input_file = nullptr;
    char * output_file = nullptr;
    while((c = getopt(argc, argv, "I:O:t:")) >= 0)
    {
        switch (c)
        {
            case 'I':
                input_file = strdup(optarg);
                break;
            
            case 'O':
                output_file = strdup(optarg);

                // 检查输入参数妥当
                if(fs::exists(output_file)){
                    fs::remove(output_file);
                }
                break;

            case 't':
                num_thread_shuffle = atoi(optarg);
                break;
                
            default:
                break;
        }
    }
    assert(output_file != nullptr);
       
    // read the header
    sam_hdr_t * header = nullptr;
    htsFile * fp = nullptr;
    if(input_file != nullptr)
    {
        fp = sam_open(input_file, "r");
        assert(strcmp("sam", hts_format_file_extension(hts_get_format(fp))) == 0);// 强制检查文件格式为 sam
        header = sam_hdr_read(fp);
    } else {
        header = sam_hdr_read_stdin();
    }
   
    BamParser::header = header;

    {
        // construct kTable
        uint64_t accumulate = 0;
        for(int i = 0; i < header->n_targets; i++){
            BAMRecord::kTable.push_back(accumulate);
            accumulate += header->target_len[i];
        }
        BAMRecord::kTable.push_back(accumulate);
    }

    // global variable
    const int num_partitions = 100;
    uint64_t max_elems_per_partition = 1024*1024*1024;
    uint64_t reference_length = BAMRecord::kTable.back();

    
    // partitioners
    BAMPartitioner bam_partitioner(reference_length, num_partitions, max_elems_per_partition);
    RangePartitioner<SinglePair> single_partitioner(reference_length, num_partitions, max_elems_per_partition);
    RangePartitioner<DoublePair> double_partitioner(reference_length, num_partitions, max_elems_per_partition);
    bitmap double_pair_indicator(2*reference_length); // 辅助根据 double pair 的信息去重 single pair
    time_stamp("program start");

    read_finished = false;
    std::thread read_thread(read_alignment, fp, header);

    size_t total_num = 0;
    std::mutex num_lock;
    

    // cache for SinglePair and DoublePair 
    SinglePairCache * singlePairCache = new SinglePairCache[num_thread_shuffle];
    DoublePairCache * doublePairCache = new DoublePairCache[num_thread_shuffle];

    tbb::parallel_for(0, num_thread_shuffle,
                          [&bam_partitioner, &single_partitioner, &double_partitioner, reference_length, &header,
                          &double_pair_indicator, &singlePairCache, &doublePairCache, &total_num, &num_lock]
                          (int i){
                            auto bbuffer = bam_partitioner.initBuffer();
                            auto sbuffer = single_partitioner.initBuffer();
                            auto dbuffer = double_partitioner.initBuffer();

                            uint64_t pairID_base = pairIDSource.fetch_add(pairIDASC);
                            uint32_t cnt_pairID = 0;

                            std::vector<kstring_t> * items = nullptr;
                            BamParser bam_parser;
                            size_t read_num = 0;
                            while(true)
                            {  
                                // if all the reads are processed, then break the loop
                                if(read_finished && LineQueue.size_approx() == 0)
                                {
                                    break;
                                }

                                LineQueue.try_dequeue(items);
                                if(items)
                                {
                                    read_num += items->size();
                                    bam_parser.add_line(items);
                                    while(bam_parser.has_record())
                                    {
                                        uint64_t pairID;
                                        if(cnt_pairID == pairIDASC){
                                            cnt_pairID = 0;
                                            pairID_base = pairIDSource.fetch_add(pairIDASC);
                                        }
                                        pairID = pairID_base + cnt_pairID;
                                        cnt_pairID++;

                                        BAMRecord * record1 = bam_parser.pop_record(pairID).release();
                                        BAMRecord * record2 = bam_parser.pop_record(pairID, record1).release();
                                        if(record2 == nullptr){
                                            //ignorable 的 single pair 没有被进行找重的必要
                                            if(record1->ignorable() == false){
                                                auto pair = new (singlePairCache[i].getSpace()) SinglePair(record1);
                                                single_partitioner.addElem(sbuffer, pair);
                                            }
                                            bam_partitioner.addElem(record1, bbuffer);
                                        }else{   
                                            auto pair = new (doublePairCache[i].getSpace()) DoublePair(record1, record2);
                                            double_partitioner.addElem(dbuffer, pair);  
                                            bam_partitioner.addElem( record1, bbuffer);
                                            bam_partitioner.addElem(record2, bbuffer);
                                            // set double_pair_indicator
                                            if(pair->get_orientation() == Orientation::FF
                                            || pair->get_orientation() == Orientation::RF){
                                                double_pair_indicator.set(pair->get_record2_prime5_pos());
                                            }else{
                                                double_pair_indicator.set(pair->get_record2_prime5_pos() + reference_length);
                                            }
                                            if(pair->get_orientation() == Orientation::FF
                                            || pair->get_orientation() == Orientation::FR){
                                                double_pair_indicator.set(pair->get_record1_prime5_pos());
                                            }else{
                                                double_pair_indicator.set(pair->get_record1_prime5_pos() + reference_length);
                                            }
                                        }
                                    }
                                    
                                } else {
                                    // give up CPU if there are no task left
                                    //std::this_thread::yield();
                                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                                }
                                
                                items = nullptr;
                            
                            }
                    
                            single_partitioner.destroyBuffer(sbuffer);
                            double_partitioner.destroyBuffer(dbuffer);
                            bam_partitioner.destroyBuffer(bbuffer);
                            num_lock.lock();
                            total_num += read_num;
                            num_lock.unlock();
                          });
    //std::cout << total_num << " reads parsed" << std::endl;
    read_thread.join();
    // close the file
    if(input_file)
    {
        sam_close(fp);
        free(input_file);
    }
        

    // flush all the data in the buffer to the file
    tbb::parallel_for(0, num_partitions, [&bam_partitioner](int i){
        BAMRecordBuffer * bam_buffer = bam_partitioner.getBAMRecordBuffer(i);
        bam_buffer->flushData();
    });

    time_stamp("shuffle done");

    std::cout << "bam record count: " << BAMRecord::count_bam_record
        << "\t" << "memory size: " << double(BAMRecord::count_bam_record) * sizeof(BAMRecord) * 2 / 1024 / 1024
        << "MB" << std::endl;
    
    bitmap duplicate_index(pairIDSource); // 存储找重的结果
    // sort double pair
    {
        auto rdds = double_partitioner.getResult();
        {
            uint64_t size = 0;
            uint64_t capacity = 0;
            for(auto &rdd : rdds){
                size += rdd.size();
                capacity += rdd.capacity();
            }
            std::cout << "size: " << double(size) / 1024 /1024 * sizeof(int*) << "MB" << "\t"
            << "capacity: " << double(capacity) / 1024 /1024 *sizeof(int*)<< "MB" << std::endl;
        }
        tbb::parallel_for(0, num_partitions,
                          [&rdds](uint32_t i){
            std::sort(rdds[i].begin(), rdds[i].end(),
                      // 因为目前 range partition 存储的是 pointer；---- 临时的
                      [](DoublePair* a, DoublePair* b){
                // record1.prime5_pos, record2.prime5_pos, pair.orientation
                if(a->compare_pos_orientation(*b) != 0){
                    return a->compare_pos_orientation(*b) == -1;
                }
                // pair score, bigger as first
                if(a->compare_score(*b) != 0){
                    return a->compare_score(*b) == 1;
                }
                // compare tile, x, y
                return a->compare_tile_X_Y(*b) != 1;
            });
        });
        time_stamp("double pair sort done");
        // search duplicate index among double pair
        {
            tbb::parallel_for(0, num_partitions,
                              [&rdds, &duplicate_index](uint32_t ii){
                auto &rdd = rdds[ii];
                for(uint64_t i = 0; i < rdd.size(); ){
                    uint64_t j;
                    for(j= i+1; j < rdd.size()
                    && rdd[i]->compare_pos_orientation(*(rdd[j])) == 0; j++){
                        duplicate_index.set(rdd[j]->get_pairID());
                    }
                    i = j;
                }
            });
        }
        delete[] doublePairCache;
        time_stamp("double pair search duplicate index done");
    }

    // sort single pair
    {
        auto rdds = single_partitioner.getResult();
        {
            uint64_t size = 0;
            uint64_t capacity = 0;
            for(auto &rdd : rdds){
                size += rdd.size();
                capacity += rdd.capacity();
            }
            std::cout << "size: " << double(size) / 1024 /1024 * sizeof(int*) << "MB" << "\t"
            << "capacity: " << double(capacity) / 1024 /1024 * sizeof(int*) << "MB" << std::endl;
        }
        tbb::parallel_for(0, num_partitions,
                          [&rdds](uint32_t i){
            std::sort(rdds[i].begin(), rdds[i].end(),
                      // 因为目前 range partition 存储 pointer --- 临时性的
                      [](SinglePair* a, SinglePair* b){
                // record.prime5_pos, pair.orientation
                if(a->compare_pos_orientation(*b) != 0){
                    return a->compare_pos_orientation(*b) == -1;
                }
                // pair score, bigger as first
                if(a->compare_score(*b) != 0){
                    return a->compare_score(*b) == 1;
                }
                // compare tile, x, y
                return a->compare_tile_X_Y(*b) != 1;
            });
        });
        time_stamp("single pair sort done");
        // search duplicate index among single pair
        {
            tbb::parallel_for(0, num_partitions,
                              [&rdds, &duplicate_index, &double_pair_indicator, reference_length](uint32_t ii){
                auto &rdd = rdds[ii];
                for(uint64_t i = 0; i < rdd.size(); ){
                    if(rdd[i]->ignorable()){
                        i++;
                        continue;
                    }
                    auto target = rdd[i]->get_prime5_pos();
                    if(rdd[i]->get_orientation() == Orientation::RR){
                        target += reference_length;
                    }
                    if(double_pair_indicator.get(target)){
                        duplicate_index.set(rdd[i]->get_pairID());
                    }
                    uint64_t j;
                    for(j = i+1; j < rdd.size() && rdd[i]->compare_pos_orientation(*(rdd[j])) == 0; j++){
                        duplicate_index.set(rdd[j]->get_pairID());
                    }
                    i = j;
                }
            });
        }
        delete[] singlePairCache;
        time_stamp("single pair search duplicate done");
    }


    // mark duplicate and output
    auto rdds = bam_partitioner.getResult();

    tbb::parallel_for(0, num_partitions,
                      [&rdds](uint32_t i){
                        std::stable_sort(rdds[i].begin(), rdds[i].end(),
                                  [](std::pair<uint64_t, size_t> a, std::pair<uint64_t, size_t> b){
                                    return a.first < b.first;
                                  });
                      });
    time_stamp("bam record sort done");

    int num_thread = std::thread::hardware_concurrency();
    int num_block = num_partitions * num_thread;

    // allocate space to store compressed data and indexes
    void * output_data[num_block];
    hts_idx_t * hts_idxes[num_block];

    for(int i=0; i<num_partitions; i++){

        // load the data from the file
        BAMRecordBuffer * bam_buffer = bam_partitioner.getBAMRecordBuffer(i);
        auto BAMRecordData = bam_buffer->readData();
        assert(BAMRecordData != nullptr);

        auto &rdd = rdds[i];

        tbb::parallel_for(0, num_thread, [&rdd, &duplicate_index, &num_thread, 
                &BAMRecordData, &output_data, &hts_idxes, &header, &output_file, &i, &total_num](uint32_t j){
        // for(int j=0; j<num_thread; j++){

            size_t read_num = 0;
            BAMRecord * record;
            uint32_t size = rdd.size() / num_thread;
            uint32_t start = j * size;
            uint32_t end = (j == (num_thread - 1)) ? rdd.size() : (j + 1) * size ;

            output_data[i * num_thread + j] = (void *)malloc(BGZF_MAX_BLOCK_SIZE); //---Don't forget to free it
            int * block_size = (int *)output_data[i * num_thread + j];
            *block_size = BGZF_MAX_BLOCK_SIZE;
            *(block_size + 1) = BGZF_MAX_BLOCK_SIZE - 3 * INT_SIZE;  // the size of remaining space
            *(block_size + 2) = 0;

            auto fp = sam_open(output_file, "wb");
            assert(sam_hdr_write(fp, header) == 0);
            fp->fp.bgzf->block_address = 0;  //---we need block_address to record the offset in the rdd block
            char *fn_out_idx = auto_index(fp, output_file, header);
            
            for(uint32_t k = start ; k < end; k++)
            {
                auto &pair = rdd[k];
                record = (BAMRecord *)(BAMRecordData + pair.second);
                if(duplicate_index.get(record->get_pairID())){
                    record->mardup();
                }

                bam1_t * b = record->get_record();
                b->data = (uint8_t *)record + BAMPartitioner::RecordSize;
                assert(bam_write_idx2(fp, header, b, &output_data[i * num_thread + j], i * num_thread + j) >= 0);
                read_num ++;
            }

            // close the file pointer for each thread
            hts_idxes[i * num_thread + j] = fp->idx;
            fp->idx = nullptr;

            // flush the data left into the compressed block    // TODO: is the index correct here?
            bgzf_flush2(fp->fp.bgzf, &output_data[i * num_thread + j]);
            assert(hts_close2(fp) == 0);
            if(fn_out_idx)
                free(fn_out_idx);

        //}
        });

        // free the space
        free(BAMRecordData);
        //std::cout << i << "th rdd traversal completed, size of rdd: " << rdd.size() << std::endl;
    }
    //std::cout << total_num << " reads written\n";
    time_stamp("mark duplicate and compress data done");

    auto output_fp = sam_open(output_file, "wb");
    assert(sam_hdr_write(output_fp, header) == 0);
    char *fn_out_idx = auto_index(output_fp, output_file, header);
    // output the header
    assert(hflush(output_fp->fp.bgzf->fp) == 0);

    merge_index(hts_idxes, num_block, output_data, output_fp->fp.bgzf->block_address);
    hts_idx_finish3(hts_idxes[0]);

    // output the compressed data
    for(int i=0; i<num_block; i++)
    {
        int * total_size = (int *)(output_data[i]);
        
        int nbytes = *(total_size) - *(total_size + 1) - 12;
        if (hwrite(output_fp->fp.bgzf->fp, (void *)(total_size + 3), nbytes) != nbytes)
        {
            printf("File write failed (wrong size)\n");
            hts_log_error("File write failed (wrong size)");
        }
        free(output_data[i]);
    }

    assert(hts_idx_save_as(hts_idxes[0], NULL, output_fp->fnidx, hts_idx_fmt(output_fp->idx)) == 0);

    for(int i=0; i<num_block; i++)
    {
        hts_idx_destroy(hts_idxes[i]);
    }

    if(fn_out_idx)
        free(fn_out_idx);
    sam_close(output_fp);
    free(output_file);

    time_stamp("output done");

    return 0;
}

// output the bam file sequentially
void output_alignment(char* output_file, const sam_hdr_t *header, std::vector<BAMPartitioner::tRDD>& rdds, int num_partitions, BAMPartitioner& bam_partitioner
    , bitmap& duplicate_index)
{
    auto fp = sam_open(output_file, "wb");
    assert(sam_hdr_write(fp, header) == 0);
    BAMRecord * record;

    for(int i=0; i<num_partitions; i++)
    {
        BAMRecordBuffer * bam_buffer = bam_partitioner.getBAMRecordBuffer(i);
        auto BAMRecordData = bam_buffer->readData();
        assert(BAMRecordData != nullptr);

        auto &rdd = rdds[i];

        for(std::pair<uint64_t, size_t>& pair : rdd)
        {
            record = (BAMRecord *)(BAMRecordData + pair.second);
            if(duplicate_index.get(record->get_pairID())){
                record->mardup();
            }
            bam1_t * b = record->get_record();
            b->data = (uint8_t *)record + BAMPartitioner::RecordSize;
            sam_write1(fp, header, b);

        }
    }

    sam_close(fp);
    time_stamp("output done");
}


//read a line from the SAM file or stdin (if fp == nullptr), without parsing
void read_alignment(htsFile *fp, sam_hdr_t * header)
{
    kstring_t line = KS_INITIALIZE;
    int read_num = 0;

    bam1_t *bam_last = bam_init1();
    bam1_t *bam_now = bam_init1();

    // vector used to store the collected line
    std::vector<kstring_t>* items = new std::vector<kstring_t>;
    items->reserve(BULK_SIZE);
    
    int (*getline_func) (htsFile *, int, kstring_t *);
    getline_func = fp ? hts_getline : getline_stdin;
    while(getline_func(fp, KS_SEP_LINE, &line) > 0)
    {
        items->push_back(line);
        assert(items->back().s == line.s);
        if(items->size() >= BULK_SIZE - 100)
        {
            // copy the line to a temporary variable
            char * temp_s = new char[line.m];
            strcpy(temp_s, line.s);
            kstring_t temp = {line.l, line.m, temp_s};
            assert(sam_parse1(&temp, header, bam_now) >= 0);
            delete[] temp.s;

            if(bam_get_qname(bam_last) != nullptr && strcmp(bam_get_qname(bam_now), bam_get_qname(bam_last)) != 0)
            {
                LineQueue.enqueue(items);

                items = new std::vector<kstring_t>;
                items->reserve(BULK_SIZE);
                free(bam_last->data);
                free(bam_now->data);
                memset(bam_last, 0, sizeof(bam1_t));
                memset(bam_now, 0, sizeof(bam1_t));
            } else {
                std::swap(bam_last, bam_now);
            }
        }
        
        line = KS_INITIALIZE;
        read_num++;
    }
    // enqueue the lines left
    LineQueue.enqueue(items);
    read_finished = true;

    //--- for debug mode
    std::cout << "read finished, number of reads: " << read_num << "\n";
    std::cout << "size of blocking queue: " << LineQueue.size_approx() << std::endl;

    // free the space
    free(line.s);
    bam_destroy1(bam_last);
    bam_destroy1(bam_now);
}

/*
 * Utility function to add an index to a file we've opened for write.
 * NB: Call this after writing the header and before writing sequences.
 *
 * The returned index filename should be freed by the caller, but only
 * after sam_idx_save has been called.
 *
 * Returns index filename on success,
 *         NULL on failure.
 */
char *auto_index(htsFile *fp, const char *fn, bam_hdr_t *header) {
    char *fn_idx;
    //int min_shift = 14; /* CSI */
    int min_shift = 0;  // BAI
    if (!fn || !*fn || strcmp(fn, "-") == 0)
        return NULL;

    fn_idx = (char *)malloc(strlen(fn)+6);
    if (!fn_idx)
        return NULL;

    sprintf(fn_idx, "%s.%s", fn, "bai");

    if (sam_idx_init(fp, header, min_shift, fn_idx) < 0) {
        printf("failed to open index \n");
        free(fn_idx);
        return NULL;
    }

    return fn_idx;
}


void time_stamp(std::string hint){
    static auto now = std::chrono::steady_clock::now();
    static auto begin = now;
    static auto last = now;
    now = std::chrono::steady_clock::now();
    std::chrono::duration<double> time1 = now - begin;
    decltype(time1) time2 = now - last;
    last = now;
    std::cout << hint << "\t" << "module elapsed " << time2.count() << "s\t"
    << "total elapsed " << time1.count() << "s\n";
}
