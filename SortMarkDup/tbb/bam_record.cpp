
#include "bam_record.h"
std::vector<uint64_t> BAMRecord::kTable;

uint64_t BAMRecord::count_bam_record = 0;

uint16_t BAMRecord::score() const{
  uint8_t* p = bam_get_qual(&record);
  uint16_t result = 0;
  // calculate a score for the  read which is the sum of scores over Q15
  for(int i = 0; i < record.core.l_qseq; i++){
    if(p[i] >= 15)
      result += p[i];
  }
  return result;
}

uint64_t BAMRecord::get_unify_coordinate() const{
  if(record.core.tid < 0){
    return kTable.back();
  }else{
    return kTable[record.core.tid] + record.core.pos;
  }
}

uint64_t BAMRecord::prime5_pos() const{
  uint32_t *cigar_array = bam_get_cigar(&(record));
  uint32_t n_cigar = record.core.n_cigar;
  auto tmp = get_unify_coordinate();
  if(n_cigar == 0){
    return tmp;
  }
  if(is_forward()){
    // tmp -= clipped_length
    for(uint32_t i = 0; i < n_cigar; i++){
      if((bam_cigar_op(cigar_array[i]) == BAM_CSOFT_CLIP) || (bam_cigar_op(cigar_array[i]) == BAM_CHARD_CLIP)){
        tmp -= bam_cigar_oplen(cigar_array[i]);
      }else{
        return tmp;
      }
    }
    return tmp;
  }else{
    // tmp += getCigar().getReferenceLength - 1 + clipped_length;
    int i = n_cigar - 1;
    // clipped_length
    while((bam_cigar_op(cigar_array[i]) == BAM_CSOFT_CLIP)
          || (bam_cigar_op(cigar_array[i]) == BAM_CHARD_CLIP)){
      tmp += bam_cigar_oplen(cigar_array[i]);
      i--;
      if(i < 0) break;
    }
    // referenceLength
    for(; i >= 0; i--){
      if((bam_cigar_type(bam_cigar_op(cigar_array[i])) & 2) != 0)
        tmp += bam_cigar_oplen(cigar_array[i]);
    }
    // -1
    tmp--;
    return tmp;
  }
}
