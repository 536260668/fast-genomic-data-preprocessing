/**
 * A class used to parse a line to a bam1_t
 * author: lhh
 */

#ifndef BAM_PARSER_H
#define BAM_PARSER_H

#include <vector>
#include <list>
#include <memory>
#include "bam_record.h"

class BamParser{
public:
    BamParser();
    ~BamParser();

    bool has_record();
    std::unique_ptr<BAMRecord> pop_record(const uint64_t pairID); // 需要小心的初始化 BAMRecord 中的各项
    std::unique_ptr<BAMRecord> pop_record(const uint64_t pairID, const BAMRecord* hint);

    void add_line(std::vector<kstring_t> * line);

    // remove everything in the vector<kstring_t> line
    void clear();

    static sam_hdr_t *header;

private:
    std::list<BAMRecord*> records;
    std::vector<kstring_t> * line;
    int index;

    // return nullptr if record file end. set pairID = 1 for !ignorale or pairID = 0 for ignorable
    BAMRecord* construct_BAMRecord();
};


#endif