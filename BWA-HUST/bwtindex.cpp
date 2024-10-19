/* The MIT License

   Copyright (c) 2008 Genome Research Ltd (GRL).

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

/* Contact: Heng Li <lh3@sanger.ac.uk> */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <zlib.h>
#include "bntseq.h"
#include "bwa.h"
#include "bwt.h"
#include "bwt_gen.h"
#include "utils.h"
#include "rle.h"
#include "rope.h"
#include <string>
#include "bwamem.h"

#ifdef _DIVBWT
#include "divsufsort.h"
#endif

#ifdef USE_MALLOC_WRAPPERS
#  include "malloc_wrap.h"
#endif

#define _get_pac(pac, l) ((pac)[(l)>>2]>>((~(l)&3)<<1)&3)
using namespace std;

int is_bwt(ubyte_t *T, int n);

int64_t bwa_seq_len(const char *fn_pac)
{
	FILE *fp;
	int64_t pac_len;
	ubyte_t c;
	fp = xopen(fn_pac, "rb");
	err_fseek(fp, -1, SEEK_END);
	pac_len = err_ftell(fp);
	err_fread_noeof(&c, 1, 1, fp);
	err_fclose(fp);
	return (pac_len - 1) * 4 + (int)c;
}

bwt_t *bwt_pac2bwt(const char *fn_pac, int use_is)
{
	bwt_t *bwt;
	ubyte_t *buf, *buf2;
	int64_t i, pac_size;
	FILE *fp;

	// initialization
	bwt = (bwt_t*)calloc(1, sizeof(bwt_t));
	bwt->seq_len = bwa_seq_len(fn_pac);
	bwt->bwt_size = (bwt->seq_len + 15) >> 4;
	fp = xopen(fn_pac, "rb");

	// prepare sequence
	pac_size = (bwt->seq_len>>2) + ((bwt->seq_len&3) == 0? 0 : 1);
	buf2 = (ubyte_t*)calloc(pac_size, 1);
	err_fread_noeof(buf2, 1, pac_size, fp);
	err_fclose(fp);
	memset(bwt->L2, 0, 5 * 4);
	buf = (ubyte_t*)calloc(bwt->seq_len + 1, 1);
	for (i = 0; i < bwt->seq_len; ++i) {
		buf[i] = buf2[i>>2] >> ((3 - (i&3)) << 1) & 3;
		++bwt->L2[1+buf[i]];
	}
	for (i = 2; i <= 4; ++i) bwt->L2[i] += bwt->L2[i-1];
	free(buf2);

	// Burrows-Wheeler Transform
	if (use_is) {
		bwt->primary = is_bwt(buf, bwt->seq_len);
	} else {
		rope_t *r;
		int64_t x;
		rpitr_t itr;
		const uint8_t *blk;

		r = rope_init(ROPE_DEF_MAX_NODES, ROPE_DEF_BLOCK_LEN);
		for (i = bwt->seq_len - 1, x = 0; i >= 0; --i) {
			int c = buf[i] + 1;
			x = rope_insert_run(r, x, c, 1, 0) + 1;
			while (--c >= 0) x += r->c[c];
		}
		bwt->primary = x;
		rope_itr_first(r, &itr);
		x = 0;
		while ((blk = rope_itr_next_block(&itr)) != 0) {
			const uint8_t *q = blk + 2, *end = blk + 2 + *rle_nptr(blk);
			while (q < end) {
				int c = 0;
				int64_t l;
				rle_dec1(q, c, l);
				for (i = 0; i < l; ++i)
					buf[x++] = c - 1;
			}
		}
		rope_destroy(r);
	}
	bwt->bwt = (uint32_t*)calloc(bwt->bwt_size, 4);
	for (i = 0; i < bwt->seq_len; ++i)
		bwt->bwt[i>>4] |= buf[i] << ((15 - (i&15)) << 1);
	free(buf);
	return bwt;
}

#define bwt_B00(b, k) ((b)->bwt[(k)>>4]>>((~(k)&0xf)<<1)&3)

void bwt_bwtupdate_core(bwt_t *bwt)
{
	bwtint_t i, k, c[4], n_occ;
	uint32_t *buf;

	n_occ = (bwt->seq_len + OCC_INTERVAL - 1) / OCC_INTERVAL + 1;
	bwt->bwt_size += n_occ * sizeof(bwtint_t); // the new size
	buf = (uint32_t*)calloc(bwt->bwt_size, 4); // will be the new bwt
	c[0] = c[1] = c[2] = c[3] = 0;
	for (i = k = 0; i < bwt->seq_len; ++i) {
		if (i % OCC_INTERVAL == 0) {
			memcpy(buf + k, c, sizeof(bwtint_t) * 4);
			k += sizeof(bwtint_t); // in fact: sizeof(bwtint_t)=4*(sizeof(bwtint_t)/4)
		}
		if (i % 16 == 0) buf[k++] = bwt->bwt[i/16]; // 16 == sizeof(uint32_t)/2
		++c[bwt_B00(bwt, i)];
	}
	// the last element
	memcpy(buf + k, c, sizeof(bwtint_t) * 4);
	xassert(k + sizeof(bwtint_t) == bwt->bwt_size, "inconsistent bwt_size");
	// update bwt
	free(bwt->bwt); bwt->bwt = buf;
}

int bwa_index(int argc, char *argv[]) // the "index" command
{
	int c, algo_type = BWTALGO_AUTO, is_64 = 0, block_size = 10000000;
    int min_seed_len = 0;
	char *prefix = 0, *str;
	while ((c = getopt(argc, argv, "6a:p:b:")) >= 0) {
		switch (c) {
		case 'a': // if -a is not set, algo_type will be determined later
			if (strcmp(optarg, "rb2") == 0) algo_type = BWTALGO_RB2;
			else if (strcmp(optarg, "bwtsw") == 0) algo_type = BWTALGO_BWTSW;
			else if (strcmp(optarg, "is") == 0) algo_type = BWTALGO_IS;
			else err_fatal(__func__, "unknown algorithm: '%s'.", optarg);
			break;
		case 'p': prefix = strdup(optarg); break;
		case '6': is_64 = 1; break;
		case 'b':
			block_size = strtol(optarg, &str, 10);
			if (*str == 'G' || *str == 'g') block_size *= 1024 * 1024 * 1024;
			else if (*str == 'M' || *str == 'm') block_size *= 1024 * 1024;
			else if (*str == 'K' || *str == 'k') block_size *= 1024;
			break;
		default: return 1;
		}
	}

	if (optind + 1 > argc) {
		fprintf(stderr, "\n");
		fprintf(stderr, "Usage:   bwa index [options] <in.fasta>\n\n");
		fprintf(stderr, "Options: -a STR    BWT construction algorithm: bwtsw, is or rb2 [auto]\n");
		fprintf(stderr, "         -p STR    prefix of the index [same as fasta name]\n");
		fprintf(stderr, "         -b INT    block size for the bwtsw algorithm (effective with -a bwtsw) [%d]\n", block_size);
		fprintf(stderr, "         -6        index files named as <in.fasta>.64.* instead of <in.fasta>.* \n");
		fprintf(stderr, "\n");
		fprintf(stderr,	"Warning: `-a bwtsw' does not work for short genomes, while `-a is' and\n");
		fprintf(stderr, "         `-a div' do not work not for long genomes.\n\n");
		return 1;
	}
	if (prefix == 0) {
        prefix = (char *) malloc(strlen(argv[optind]) + 4);
        strcpy(prefix, argv[optind]);
        if (is_64) strcat(prefix, ".64");
    }
	//bwa_idx_build(argv[optind], prefix, algo_type, block_size);

    if(min_seed_len == 0)   min_seed_len = 19;
    lisa_idx_build(argv[optind], prefix, min_seed_len, 0, "./");

	free(prefix);
	return 0;
}

int64_t pac_seq_len(const char *fn_pac)
{
    FILE *fp;
    int64_t pac_len;
    uint8_t c;
    fp = xopen(fn_pac, "rb");
    err_fseek(fp, -1, SEEK_END);
    pac_len = err_ftell(fp);
    err_fread_noeof(&c, 1, 1, fp);
    err_fclose(fp);
    return (pac_len - 1) * 4 + (int)c;
}

void read_seq_lisa(std::string filename, std::string &reference_seq){
    uint8_t *buf2;
    int64_t i, pac_size, seq_len;
    FILE *fp;
    filename += ".pac";
    const char *fn_pac = filename.c_str();

    // initialization
    seq_len = pac_seq_len(fn_pac);
    assert(seq_len > 0);
    assert(seq_len <= 0x7fffffffffL);
    fp = xopen(fn_pac, "rb");

    // prepare sequence
    pac_size = (seq_len>>2) + ((seq_len&3) == 0? 0 : 1);
    buf2 = (uint8_t*)calloc(pac_size, 1);
    assert(buf2 != NULL);
    err_fread_noeof(buf2, 1, pac_size, fp);
    err_fclose(fp);
    for (i = 0; i < seq_len; ++i) {
        int nt = buf2[i>>2] >> ((3 - (i&3)) << 1) & 3;
        switch(nt)
        {
            case 0:
            reference_seq += "A";
            break;
            case 1:
            reference_seq += "C";
            break;
            case 2:
            reference_seq += "G";
            break;
            case 3:
            reference_seq += "T";
            break;
            default:
            fprintf(stderr, "ERROR! Value of nt is not in 0,1,2,3!");
            exit(EXIT_FAILURE);
        }
    }
    free(buf2);
}

//int lisa_idx_build(const char *fa, const char *prefix, int min_seed_len, uint64_t num_rmi_leaf, string mem2_home)
//{
//    extern void bwa_pac_rev_core(const char *fn, const char *fn_rev);
//
//    clock_t t;
//    int64_t l_pac;
//    // nucleotide indexing
//    gzFile fp = xzopen(fa, "r");
//    t = clock();
//    fprintf(stderr, "[lisa_index] Pack FASTA... ");
//    l_pac = bns_fasta2bntseq_lisa(fp, prefix, 1);
//    err_gzclose(fp);
//    FMI_search *fmi = new FMI_search(prefix);
//    fmi->build_index(0);
//    delete fmi;
//
//    string ref_seq_file = (string) prefix;
//
//    string seq;
//    {
//        gzFile fp = xzopen(ref_seq_file.c_str(), "r");
//        read_seq_lisa(ref_seq_file, seq);
//        fprintf(stderr, "Read ref file done.\n");
//    }
//    string path = mem2_home + "/ext/TAL";
//    LISA_search<index_t> *lisa =  new LISA_search<index_t>(seq, seq.size(), ref_seq_file, min_seed_len + 1, num_rmi_leaf, path);
//
//    delete lisa;
//    fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
//
//    return 0;
//}

//---use the same way to build index as bwa
int lisa_idx_build(const char *fa, const char *prefix, int min_seed_len, uint64_t num_rmi_leaf, string mem2_home)
{
    extern void bwa_pac_rev_core(const char *fn, const char *fn_rev);

    int algo_type = BWTALGO_AUTO;
    int block_size = 10000000;

    char *str, *str2, *str3;
    clock_t t;
    int64_t l_pac;

    str  = (char*)calloc(strlen(prefix) + 10, 1);
    str2 = (char*)calloc(strlen(prefix) + 10, 1);
    str3 = (char*)calloc(strlen(prefix) + 10, 1);

    { // nucleotide indexing
        gzFile fp = xzopen(fa, "r");
        t = clock();
        if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Pack FASTA... ");
        /* traverse a fasta file to a binary file; and return the number of nucleotide
generate that binary file.  */
        l_pac = bns_fasta2bntseq(fp, prefix, 2);//both reverse complete and dupicate for cycle rotation
        if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
        err_gzclose(fp);
    }
    if (algo_type == 0) algo_type = l_pac > 50000000? 2 : 3; // set the algorithm for generating BWT
    {
        strcpy(str, prefix); strcat(str, ".pac");
        strcpy(str2, prefix); strcat(str2, ".bwt");
        t = clock();
        if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Construct BWT for the packed sequence...\n");
        bwt_bwtgen2(str, str2, block_size);
        if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] %.2f seconds elapse.\n", (float)(clock() - t) / CLOCKS_PER_SEC);
    }
    {
        bwt_t *bwt;
        strcpy(str, prefix); strcat(str, ".bwt");
        t = clock();
        if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Update BWT... ");
        bwt = bwt_restore_bwt(str);
        bwt_bwtupdate_core(bwt);
        bwt_dump_bwt(str, bwt);
        bwt_destroy(bwt);
        if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
    }
    {
        bwt_t *bwt;
        strcpy(str, prefix); strcat(str, ".bwt");
        strcpy(str2, prefix); strcat(str2, ".pac");
        strcpy(str3, prefix); strcat(str3, ".sa");
        t = clock();
        if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Construct SA and Occ... ");
        bwt = bwt_restore_bwt(str);
        bwt_cal_sa_and_sample(bwt);

        FILE *fp_pac=xopen(str2,"rb");
        char *pac= (char*)calloc(l_pac/4+1,1);
        if(pac==NULL){
            printf("bwa_index_build error:: cannot allocate enough memory\n");
        }
        err_fread_noeof(pac,1,l_pac/4+1,fp_pac);
        err_fclose(fp_pac);

        lbwt_t lbwt;
        lbwt.refLen=bwt->seq_len/4;
        free(bwt->bwt);
        constructOccArray(&lbwt,pac,bwt);
        bwt_dump_sa_lambert(str3, bwt);
        free(bwt->sa);free(bwt);
        lbwt_dump_lbwt(str,&lbwt);

        //bwt_destroy(bwt);
        //free(pac);
        free(lbwt.occArray);
        if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);

        //---build lisa index
        string ref_seq_file = (string) prefix;
        string seq;

        for(int64_t i=0; i<lbwt.refLen; i++)
        {
            seq.push_back(dna[_get_pac(pac, i)]);
        }

        string path = mem2_home + "/ext/TAL";
        LISA_search<index_t> *lisa =  new LISA_search<index_t>(seq, seq.size(), ref_seq_file, min_seed_len + 1, num_rmi_leaf, path);

        delete lisa;
        free(pac);
    }
    {
        gzFile fp = xzopen(fa, "r");
        t = clock();
        if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Pack forward-only FASTA... ");
        l_pac = bns_fasta2bntseq(fp, prefix, 1);
        if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
        err_gzclose(fp);
    }
    free(str3); free(str2); free(str);


//    t = clock();
//    fprintf(stderr, "[lisa_index] Pack FASTA... ");
//    l_pac = bns_fasta2bntseq_lisa(fp, prefix, 1);
//    err_gzclose(fp);
//    FMI_search *fmi = new FMI_search(prefix);
//    fmi->build_index(0);
//    delete fmi;




    //fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);


    return 0;
}

int bwa_idx_build(const char *fa, const char *prefix, int algo_type, int block_size)
{
	extern void bwa_pac_rev_core(const char *fn, const char *fn_rev);

	char *str, *str2;
    char *str3;
	clock_t t;
	int64_t l_pac;

	str  = (char*)calloc(strlen(prefix) + 10, 1);
	str2 = (char*)calloc(strlen(prefix) + 10, 1);
	str3 = (char*)calloc(strlen(prefix) + 10, 1);

	{ // nucleotide indexing
		gzFile fp = xzopen(fa, "r");
		t = clock();
		if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Pack FASTA... ");
		/* traverse a fasta file to a binary file; and return the number of nucleotide 
generate that binary file.  */
		l_pac = bns_fasta2bntseq(fp, prefix, 2);//both reverse complete and dupicate for cycle rotation
		if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
		err_gzclose(fp);
	}
	if (algo_type == 0) algo_type = l_pac > 50000000? 2 : 3; // set the algorithm for generating BWT
	{
		strcpy(str, prefix); strcat(str, ".pac");
		strcpy(str2, prefix); strcat(str2, ".bwt");
		t = clock();
		if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Construct BWT for the packed sequence...\n");
	        bwt_bwtgen2(str, str2, block_size);
		if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] %.2f seconds elapse.\n", (float)(clock() - t) / CLOCKS_PER_SEC);
	}
	{
		bwt_t *bwt;
		strcpy(str, prefix); strcat(str, ".bwt");
		t = clock();
		if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Update BWT... ");
		bwt = bwt_restore_bwt(str);
		bwt_bwtupdate_core(bwt);
		bwt_dump_bwt(str, bwt);
		bwt_destroy(bwt);
		if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
	}
	{
		bwt_t *bwt;
		strcpy(str, prefix); strcat(str, ".bwt");
		strcpy(str2, prefix); strcat(str2, ".pac");
		strcpy(str3, prefix); strcat(str3, ".sa");
		t = clock();
		if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Construct SA and Occ... ");
		bwt = bwt_restore_bwt(str);
		bwt_cal_sa_and_sample(bwt);
		
		FILE *fp_pac=xopen(str2,"rb");
		char *pac=(char*)calloc(l_pac/4+1,1);
		if(pac==NULL){
		  printf("bwa_index_build error:: cannot allocate enough memory\n");
		}
		err_fread_noeof(pac,1,l_pac/4+1,fp_pac);
		err_fclose(fp_pac);

		lbwt_t lbwt;
		lbwt.refLen=bwt->seq_len/4;
		free(bwt->bwt);
		constructOccArray(&lbwt,pac,bwt);
		bwt_dump_sa_lambert(str3, bwt);

        free(bwt->sa);free(bwt);
		lbwt_dump_lbwt(str,&lbwt);

        // build lcp tree
        std::string ref_seq_file(prefix);


        // transform packed reference to string
        std::string seq;
        {
            for(int64_t i=0; i<lbwt.refLen; i++)
            {
                seq.push_back(dna[_get_pac(pac, i)]);
            }
        }
        std::string path = "/ext/TAL";
        int min_seed_len = 19;
        int num_rmi_leaf = 0;
        LISA_search<index_t> *lisa =  new LISA_search<index_t>(seq, seq.size(), ref_seq_file, min_seed_len + 1, num_rmi_leaf, path);

		//bwt_destroy(bwt);
		free(pac);
		free(lbwt.occArray);
		if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
	}
	{
		gzFile fp = xzopen(fa, "r");
		t = clock();
		if (bwa_verbose >= 3) fprintf(stderr, "[bwa_index] Pack forward-only FASTA... ");
		l_pac = bns_fasta2bntseq(fp, prefix, 1);
		if (bwa_verbose >= 3) fprintf(stderr, "%.2f sec\n", (float)(clock() - t) / CLOCKS_PER_SEC);
		err_gzclose(fp);
	}
	free(str3); free(str2); free(str);
	return 0;
}
