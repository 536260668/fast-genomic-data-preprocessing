/**
 * The implementation of BAMRecordBuffer class
 * author: lhh
 */

#include <iostream>
#include <exception>
#include <cassert>
#include <thread>
#include <filesystem>
#include <utility>
#include "concurrentqueue.h"
#include "BAMRecordBuffer.h"

namespace fs = std::filesystem;
moodycamel::ConcurrentQueue<struct CompressedBlock> CompressedBuffer;
std::atomic_bool load_finished;

BAMRecordBuffer::BAMRecordBuffer(const std::string &db_file) : 
                    buffer_offset(0), file_offset(0), file_name_(db_file)
{
    BAMBuffer = (char *)calloc(BAM_BUFFER_SIZE, sizeof(char));
    compressed_buffer = (char *)malloc(LZ4_compressBound(BAM_BUFFER_SIZE) * sizeof(char));
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);

    // directory or file does not exist
    if (!db_io_.is_open()) {
        db_io_.clear();
        // create a new file
        db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
        db_io_.close();
        // reopen with original mode
        db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
        try{
            if (!db_io_.is_open()) {
                throw "can't open db file";
            }
        }
        catch( const char* msg ){
            std::cerr << msg << std::endl;
            exit(EXIT_FAILURE);
        }

    }

}

BAMRecordBuffer::~BAMRecordBuffer()
{
    // remove the temporary files
    if(fs::exists(file_name_)){
        fs::remove(file_name_);
    }
}

size_t BAMRecordBuffer::addData(BAMRecord* elem)
{
    size_t compressed_length;
    bam1_t * b = elem->get_record();
    // change page if necessary
    if(buffer_offset + RecordSize + b->l_data > BAM_BUFFER_SIZE)
    {
        
        compressed_length = LZ4_compress_default(BAMBuffer, compressed_buffer, buffer_offset, BAM_BUFFER_SIZE);
        assert(compressed_length > 0);
        db_io_.write((const char*)compressed_buffer, compressed_length);
        

         // check for I/O error
        if (db_io_.bad()) {
            std::cerr << "I/O error while writing" << std::endl;
            exit(EXIT_FAILURE);
        }

        // clear the buffer
        memset(BAMBuffer, 0, BAM_BUFFER_SIZE);

        compressed_lengthes.push_back((size_t)compressed_length);
        offset.push_back((size_t)file_offset);
        
        file_offset += buffer_offset;
        buffer_offset = 0;
    }
    memcpy(BAMBuffer + buffer_offset, elem, RecordSize);
    memcpy(BAMBuffer + buffer_offset + RecordSize, b->data, b->l_data);
    uint32_t length = ((uint32_t)b->l_data + 7) & (~7U);
    size_t real_offset = file_offset + buffer_offset;
    buffer_offset = buffer_offset + RecordSize + length;
    
    return real_offset;
}

void BAMRecordBuffer::flushData()
{
    size_t compressed_length;
    compressed_length = LZ4_compress_default(BAMBuffer, compressed_buffer, buffer_offset, BAM_BUFFER_SIZE);
    assert(compressed_length > 0);
    db_io_.write((const char *)compressed_buffer, compressed_length);
    

    // check for I/O error
    if (db_io_.bad()) {
        std::cerr << "I/O error while writing" << std::endl;
        exit(EXIT_FAILURE);
    }
    // needs to flush to keep disk file in sync
    db_io_.flush(); 

    compressed_lengthes.push_back((size_t)compressed_length);
    offset.push_back((size_t)file_offset);
    file_offset += buffer_offset;

    offset.push_back((size_t)file_offset);
    free(BAMBuffer);
    free(compressed_buffer);

}

unsigned char * BAMRecordBuffer::readData()
{
    int num_consumer = 3;   // TODO: change this variable to a member variable
    // file_offset represents the length of the uncompressed file now
    unsigned char * buffer = (unsigned char *)calloc(file_offset, 1);

    db_io_.seekp(0);
    load_finished = false;
    std::vector<std::thread> consumer;
    for(int i=0; i<num_consumer; i++)
    {
        consumer.emplace_back(std::thread(&BAMRecordBuffer::decompress, this));
    }

    for(int i=0; i<compressed_lengthes.size(); i++)
    {
        char * compressed_buffer = (char *)malloc(compressed_lengthes[i]);
        db_io_.read((char *)compressed_buffer, compressed_lengthes[i]);
        
        struct CompressedBlock block;
        block.src = compressed_buffer;
        block.dst = (char *)(buffer + offset[i]);
        block.compressedSize = compressed_lengthes[i];
        block.dstCapacity = (int)(offset[i+1] - offset[i]);

        // enqueue the pointer
        assert(CompressedBuffer.enqueue(block));
    }
    load_finished = true;
    
    // for debugging
    // std::cout << "size of compressed_lengthes: " << compressed_lengthes.size() << "\n";
    // std::cout << "size of blocking queue: " << CompressedBuffer.size_approx() << "\n";

    for(int i=0; i<num_consumer; i++)
    {
        consumer[i].join();
    }

    if (!db_io_.good()) {
        std::cerr << "I/O error while writing" << std::endl;
        std::abort();
    }

    // if file ends before the end
    std::streamsize read_count = db_io_.gcount();
    // if (read_count < file_offset) {
    //   std::cerr << "Read less than a page " << read_count << "\t" << file_offset << std::endl;
    //   db_io_.clear();
    //   std::abort();
    // }

    return buffer;

}

/**
 * @brief dequeue a block of compressed data and decompress it, until the concurrent queue is empty
 * 
 */

void BAMRecordBuffer::decompress()
{
    int uncompressed_length;
    struct CompressedBlock block;
    while(true)
    {
        // if all the reads are processed, then break the loop
        if(load_finished && CompressedBuffer.size_approx() == 0)
        {
            break;
        }

        if(CompressedBuffer.try_dequeue(block))
        {
            // pay attention! LZ4_decompress_safe() requires the data type to be int
            uncompressed_length = LZ4_decompress_safe(block.src, block.dst, block.compressedSize, block.dstCapacity);
            assert(uncompressed_length >= 0);
            free(block.src);
        }

    }
}
