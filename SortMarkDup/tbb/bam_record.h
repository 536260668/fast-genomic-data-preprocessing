#ifndef BAM_RECORD_HH
#define BAM_RECORD_HH

#include <vector>
#include "sam.h"

// 复用 bam1_t 的 id 作为 pairID
// 不存储 unify_coordinate, 采用实时转化的策略
// ignorable 的BAMrecord 的唯一特征是 pairID = 0
class BAMRecord{
public:
  static uint64_t count_bam_record;
  uint64_t get_pairID()const {return record.id;}
  void set_pairID(uint64_t id){record.id = id;}
  // uint64_t get_pairID() const{return pairID;}
  // void set_pairID(uint64_t id){pairID = id;}
  uint64_t partition_key()const {return get_unify_coordinate();}
  uint64_t sort_key() const {return get_unify_coordinate();}
  void mardup(){record.core.flag |= BAM_FDUP;}
  bool ignorable()const{return get_pairID() == 0;} // unmapped/secondary/supplementary
  bool is_forward() const{return (record.core.flag & BAM_FREVERSE) == 0;}
  const char* qname() const{return bam_get_qname(&record);}
  BAMRecord(){memset(&record, 0, sizeof(bam1_t)); count_bam_record++;}
  ~BAMRecord(){bam_destroy1(&record);}
  static std::vector<uint64_t> kTable; // combine RNAME and POS to unified coordinate
  uint16_t score() const;
  uint64_t prime5_pos() const;
  bam1_t* get_record() {return &record;}
  friend class BamParser;
private:
  bam1_t record;
  //uint64_t pairID;
  uint64_t get_unify_coordinate() const;
};

#endif
