//
// Created by lambert on 4/20/23.
//

#ifndef BWA_MEM_BWT_GEN_H
#define BWA_MEM_BWT_GEN_H

#include "bwt.h"

void bwt_bwtgen(const char *fn_pac, const char *fn_bwt); // from BWT-SW
void bwt_bwtgen2(const char *fn_pac, const char *fn_bwt, int block_size); // from BWT-SW
void bwt_bwtgen3(char *fn_pac, char *fn_bwt, int block_size); // from BWT-SW

#endif //BWA_MEM_BWT_GEN_H
