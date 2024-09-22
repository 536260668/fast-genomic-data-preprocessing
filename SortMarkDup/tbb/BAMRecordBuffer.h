/**
 * A class used to store the BAMRecord in a file
 * author: lhh
 */

#ifndef BAMRECORD_BUFFER_H
#define BAMRECORD_BUFFER_H

#include <fstream>
#include <string>
#include <vector>
#include <lz4.h>
#include <mutex>
#include "sam.h"
#include "bam_record.h"

#define BAM_BUFFER_SIZE 0x8000000   // Maybe it can be a parameter

// some variable passed to LZ4_decompress_safe()
struct CompressedBlock
{
    char * src;
    char * dst;
    int compressedSize;
    int dstCapacity;
};


class BAMRecordBuffer
{
private:
    const static int RecordSize = sizeof(BAMRecord);
    char * BAMBuffer; // the buffer used to store the BAMRecord into the file
    size_t buffer_offset;   // the offset in the buffer
    size_t file_offset;   // the offset of the total file
    
    char * compressed_buffer;   // the buffer used to store the compressed data
    
    std::vector<size_t> compressed_lengthes;    // the size of each compressed block
    std::vector<size_t> offset;     // the accumulated size of uncompressed data

    std::fstream db_io_;
    std::string file_name_;

    void decompress();

public:
    

    BAMRecordBuffer(const std::string &db_file);
    ~BAMRecordBuffer();

    // add a BAMRecord class object into the buffer
    size_t addData(BAMRecord* elem);

    // flush all the data into the file
    void flushData();

    // read the data from the file, remember to free it
    unsigned char * readData();
};


#endif