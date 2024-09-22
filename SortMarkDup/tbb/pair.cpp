#include "pair.h"
#include <cstring>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <array>

uint64_t SinglePair::count_single_pair = 0;
uint64_t DoublePair::count_double_pair = 0;

uint16_t str_to_uint16(const char *str) {
  char *end;
  errno = 0;
  long val = strtol(str, &end, 10);
  if (errno || end == str || *end != '\0' || val < 0 || val >= 0x10000) {
    assert( true ); // 用 uint16_t 表示 tile, X, Y 位宽不够
  }
  return uint16_t(val);
}

// result[0] as tile, result[1] as x, result[2] as y
static void get_tile_x_y(const char* qname, std::array<uint16_t, 3> &result){
  auto dupstr = strdup(qname);
  char *fake_str = NULL;
  char **saveptr = &fake_str;
  std::vector<char*> vec;
  static const char* delim = ":";

  for(char* token = strtok_r(dupstr, delim, saveptr); token != NULL; token = strtok_r(NULL, delim, saveptr))
    vec.push_back(token);
  if(vec.size() == 7) // illumina's qname: instrument:run_number:flowcell_ID:lane:tile:X:Y
  {
    result[0] = str_to_uint16(vec[4]);
    result[1] = str_to_uint16(vec[5]);
    result[2] = str_to_uint16(vec[6]);
  } 
  else if(vec.size() == 6) // illumina's qname   instrument:run_number:flowcell_ID:lane:tile:X:Y
  {
    result[0] = str_to_uint16(vec[3]);
    result[1] = str_to_uint16(vec[4]);
    result[2] = str_to_uint16(vec[5]);
  }
  else {
    result[0] = 0;
    result[1] = 0;
    result[2] = 0;
  }
  free(dupstr);
}

SinglePair::SinglePair(BAMRecord* b){
  // pairID
  pairID = b->get_pairID();
  // score, tile, X, Y
  std::array<uint16_t, 3> result;
  get_tile_x_y(b->qname(), result);
  tile = result[0];
  X = result[1];
  Y = result[2];
  score = b->score();
  // sort_key
  sort_key = (b->prime5_pos() << 2);
  if(b->is_forward()){
    sort_key += Orientation::FF;
  }else{
    sort_key += Orientation::RR;
  }
  count_single_pair++;
}

DoublePair::DoublePair(BAMRecord* b1, BAMRecord* b2){
  // pairID
  pairID = b1->get_pairID();
  assert(pairID != 0);
  // score, tile, X, Y
  std::array<uint16_t, 3> result;
  get_tile_x_y(b1->qname(), result);
  tile = result[0];
  X = result[1];
  Y = result[2];
  score = b1->score() + b2->score();
  // sort_key, record2_prime5_pos
  if(b1->prime5_pos() > b2->prime5_pos()){
    std::swap(b1, b2);
  }
  sort_key = b1->prime5_pos();
  record2_prime5_pos = b2->prime5_pos();
  Orientation orientation;
  if(b1->is_forward()){
    if(b2->is_forward()){
      orientation = Orientation::FF;
    }else{
      orientation = Orientation::FR;
    }
  }else{
    if(b2->is_forward()){
      orientation = Orientation::RF;
    }else{
      orientation = Orientation::RR;
    }
  }
  if(sort_key == record2_prime5_pos && orientation == Orientation::RF){
    orientation = Orientation::FR;
  }
  sort_key <<= 2;
  sort_key += orientation;
  count_double_pair++;
}

int SinglePair::compare_pos_orientation(const SinglePair& other) const{
  if(sort_key == other.sort_key){
    return 0;
  }else if(sort_key < other.sort_key){
    return -1;
  }else{
    return 1;
  }
}

int SinglePair::compare_score(const SinglePair& other) const{
  if(score == other.score){
    return 0;
  }else if(score < other.score){
    return -1;
  }else{
    return 1;
  }
}

int SinglePair::compare_tile_X_Y(const SinglePair& other) const{
  if(tile < other.tile){
    return -1;
  }else if(tile > other.tile){
    return 1;
  }else if(X < other.X){
    return -1;
  }else if(X > other.X){
    return 1;
  }else if(Y < other.Y){
    return -1;
  }else if(Y > other.Y){
    return 1;
  }else{
    return 0;
  }
}


int DoublePair::compare_pos_orientation(const DoublePair& other) const{
  if(sort_key < other.sort_key){
    return -1;
  }else if(sort_key > other.sort_key){
    return 1;
  }else if(record2_prime5_pos < other.record2_prime5_pos){
    return -1;
  }else if(record2_prime5_pos > other.record2_prime5_pos){
    return 1;
  }
  return 0;
}

int DoublePair::compare_score(const DoublePair& other) const{
  if(score == other.score){
    return 0;
  }else if(score < other.score){
    return -1;
  }else{
    return 1;
  }
}

int DoublePair::compare_tile_X_Y(const DoublePair& other) const{
  if(tile < other.tile){
    return -1;
  }else if(tile > other.tile){
    return 1;
  }else if(X < other.X){
    return -1;
  }else if(X > other.X){
    return 1;
  }else if(Y < other.Y){
    return -1;
  }else if(Y > other.Y){
    return 1;
  }else{
    return 0;
  }
}
