/**
 * @file SAMRead.h
 * @author lhh
 * @brief include some method to parse text from the stdin rather than a SAM/BAM file
 * @date 2022-02-15
 * 
 */

#pragma once

#include "sam.h"

/**
 * @brief Overrite sam_hdr_read() method in htslib. read the header from stdin instead of the SAM/BAM file.
 *        By default, this method only read SAM format, not bgzf file.
 * 
 * @return sam_hdr_t* the pointer to the parsed header
 */
sam_hdr_t * sam_hdr_read_stdin();

/**
 * @brief  overrite hts_getline() method in htslib. read a line from stdin instead of the SAM/BAM file.
 *         encapsulate getline() method in this method.
 * 
 * @return the length of a line, excluding the terminating null byte, -1 on failure to read a line.
 */
int getline_stdin(htsFile *fp, int delimiter, kstring_t *s);