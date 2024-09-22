#ifndef PAIR_HH
#define PAIR_HH

#include "bam_record.h"

enum Orientation
  {
   FF = 0, // record1 forward - record2 forward
   FR = 1,
   RF = 2, // record1 reverse - record2 forward
   RR = 3
  };

// struct PairRecord{
//   uint64_t prime5_pos;
//   uint16_t orientation;
//   uint16_t score;
//   uint32_t tile, X, Y; // 假定 32 位无符号数能够表示 tile, X, Y
// };

// 假设 tile, x, y 可以用 uint16_t 表示
// 假设 score 值可以用 uint16_t 表示


// combine prime5_pos and orientation as sort_key, orientation 占低两位
// for single pair, Orientation::FF for forward, Orientation::RR for reverse
// consistence with BAMRecord, pairID = 0 indicate a ignorable BAMRecord
class SinglePair{
public:
  static uint64_t count_single_pair;
  SinglePair(BAMRecord*);
  uint64_t get_prime5_pos() const {return sort_key>>2;}
  // if forward, return FF, if reverse, return RR
  Orientation get_orientation() const {return Orientation(sort_key & 3);}
  bool ignorable() const {return pairID == 0;}
  uint64_t get_pairID() const {return pairID;}
  uint64_t partition_key()const{return get_prime5_pos();}
  // return 0 if equal, 1 this > other, -1, this < other
  int compare_pos_orientation(const SinglePair& other)const;
  // return 0 if equal, 1 this > other, -1, this < other
  int compare_score(const SinglePair& other)const;
  // return 0 if equal, 1 this > other, -1, this < other
  int compare_tile_X_Y(const SinglePair& other)const;
private:
  uint64_t pairID;
  uint64_t sort_key;
  uint16_t score, tile, X, Y;
};

class DoublePair{
public:
  static uint64_t count_double_pair;
  DoublePair(BAMRecord* record1, BAMRecord* record2);
  uint64_t get_pairID() const{return pairID;}
  uint64_t get_record1_prime5_pos()const{return sort_key >> 2;}
  uint64_t get_record2_prime5_pos()const{return record2_prime5_pos;}
  uint64_t partition_key()const{return get_record1_prime5_pos();}
  Orientation get_orientation()const{return Orientation(sort_key & 3);}
  // return 0 if equal, 1 this > other, -1, this < other
  int compare_pos_orientation(const DoublePair& other)const;
  // return 0 if equal, 1 this > other, -1, this < other
  int compare_score(const DoublePair& other)const;
  // return 0 if equal, 1 this > other, -1, this < other
  int compare_tile_X_Y(const DoublePair& other)const;
private:
  uint64_t pairID;
  uint64_t sort_key;
  uint16_t score, tile, X, Y;
  uint64_t record2_prime5_pos;
};

#endif
