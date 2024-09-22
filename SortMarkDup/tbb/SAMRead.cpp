/**
 * @file SAMRead.cpp
 * @author lhh
 * @brief The implementation of the methods in SAMRead.h
 * 
 */

#include <iostream>
#include <string>
#include <cstring>
#include "khash.h"
#include "hts_log.h"
#include "SAMRead.h"

KHASH_INIT2(s2i,, kh_cstr_t, int64_t, 1, kh_str_hash_func, kh_str_hash_equal)

using namespace std;

// Minimal sanitisation of a header to ensure.
// - null terminated string.
// - all lines start with @ (also implies no blank lines).
//
// Much more could be done, but currently is not, including:
// - checking header types are known (HD, SQ, etc).
// - syntax (eg checking tab separated fields).
// - validating n_targets matches @SQ records.
// - validating target lengths against @SQ records.
static sam_hdr_t *sam_hdr_sanitise(sam_hdr_t *h) {
    if (!h)
        return NULL;

    // Special case for empty headers.
    if (h->l_text == 0)
        return h;

    size_t i;
    unsigned int lnum = 0;
    char *cp = h->text, last = '\n';
    for (i = 0; i < h->l_text; i++) {
        // NB: l_text excludes terminating nul.  This finds early ones.
        if (cp[i] == 0)
            break;

        // Error on \n[^@], including duplicate newlines
        if (last == '\n') {
            lnum++;
            if (cp[i] != '@') {
                hts_log_error("Malformed SAM header at line %u", lnum);
                sam_hdr_destroy(h);
                return NULL;
            }
        }

        last = cp[i];
    }

    if (i < h->l_text) { // Early nul found.  Complain if not just padding.
        size_t j = i;
        while (j < h->l_text && cp[j] == '\0') j++;
        if (j < h->l_text)
            hts_log_warning("Unexpected NUL character in header. Possibly truncated");
    }

    // Add trailing newline and/or trailing nul if required.
    if (last != '\n') {
        hts_log_warning("Missing trailing newline on SAM header. Possibly truncated");

        if (h->l_text < 2 || i >= h->l_text - 2) {
            if (h->l_text >= SIZE_MAX - 2) {
                hts_log_error("No room for extra newline");
                sam_hdr_destroy(h);
                return NULL;
            }

            cp = (char *)realloc(h->text, (size_t) h->l_text+2);
            if (!cp) {
                sam_hdr_destroy(h);
                return NULL;
            }
            h->text = cp;
        }
        cp[i++] = '\n';

        // l_text may be larger already due to multiple nul padding
        if (h->l_text < i)
            h->l_text = i;
        cp[h->l_text] = '\0';
    }

    return h;
}


sam_hdr_t * sam_hdr_read_stdin()
{
    string line;
    kstring_t str = { 0, 0, NULL };
    khint_t k;
    sam_hdr_t* h = sam_hdr_init();
    sam_hdr_t * result = nullptr;
    const char *q, *r;
    char* sn = NULL;
    khash_t(s2i) *d = kh_init(s2i);
    khash_t(s2i) *long_refs = NULL;
    
    int ret, has_SQ = 0;
    int next_c = '@';

    if (!h || !d)
        goto error;
    while (next_c == '@' && getline(cin, line)) {
        if (line[0] != '@')
            break;

        if (line.length() > 3 && strncmp(line.data(), "@SQ", 3) == 0) {
            has_SQ = 1;
            hts_pos_t ln = -1;
            for (q = line.data() + 4;; ++q) {
                if (strncmp(q, "SN:", 3) == 0) {
                    q += 3;
                    for (r = q;*r != '\t' && *r != '\n' && *r != '\0';++r);

                    if (sn) {
                        hts_log_warning("SQ header line has more than one SN: tag");
                        free(sn);
                    }
                    sn = (char*)calloc(r - q + 1, 1);
                    if (!sn)
                        goto error;

                    strncpy(sn, q, r - q);
                    q = r;
                } else {
                    if (strncmp(q, "LN:", 3) == 0)
                        ln = strtoll(q + 3, (char**)&q, 10);
                }

                while (*q != '\t' && *q != '\n' && *q != '\0')
                    ++q;
                if (*q == '\0' || *q == '\n')
                    break;
            }
            if (sn) {
                if (ln >= 0) {
                    int absent;
                    k = kh_put(s2i, d, sn, &absent);
                    if (absent < 0)
                        goto error;

                    if (!absent) {
                        hts_log_warning("Duplicated sequence '%s'", sn);
                        free(sn);
                    } else {
                        if (ln >= UINT32_MAX) {
                            // Stash away ref length that
                            // doesn't fit in target_len array
                            int k2;
                            if (!long_refs) {
                                long_refs = kh_init(s2i);
                                if (!long_refs)
                                    goto error;
                            }
                            k2 = kh_put(s2i, long_refs, sn, &absent);
                            if (absent < 0)
                                goto error;
                            kh_val(long_refs, k2) = ln;
                            kh_val(d, k) = ((int64_t) (kh_size(d) - 1) << 32
                                            | UINT32_MAX);
                        } else {
                            kh_val(d, k) = (int64_t) (kh_size(d) - 1) << 32 | ln;
                        }
                    }
                } else {
                    hts_log_warning("Ignored @SQ SN:%s : bad or missing LN tag", sn);
                    free(sn);
                }
            } else {
                hts_log_warning("Ignored @SQ line with missing SN: tag");
            }
            sn = NULL;
        }
        if (kputsn(line.data(), line.length(), &str) < 0)
            goto error;

        if (kputc('\n', &str) < 0)
            goto error;

        //---only for SAM format now
        // if (fp->is_bgzf) {
        //     next_c = bgzf_peek(fp->fp.bgzf);
        // } else {
            unsigned char nc = cin.peek();
            if(nc == EOF)
                goto error;
            else
                next_c = nc;
        // }
            
    }

    if (ret < -1)
        goto error;

    if (has_SQ) {
        // Populate the targets array
        h->n_targets = kh_size(d);

        h->target_name = (char**) malloc(sizeof(char*) * h->n_targets);
        if (!h->target_name) {
            h->n_targets = 0;
            goto error;
        }

        h->target_len = (uint32_t*) malloc(sizeof(uint32_t) * h->n_targets);
        if (!h->target_len) {
            h->n_targets = 0;
            goto error;
        }

        for (k = kh_begin(d); k != kh_end(d); ++k) {
            if (!kh_exist(d, k))
                continue;

            h->target_name[kh_val(d, k) >> 32] = (char*) kh_key(d, k);
            h->target_len[kh_val(d, k) >> 32] = kh_val(d, k) & 0xffffffffUL;
            kh_val(d, k) >>= 32;
        }
    }

    // Repurpose sdict to hold any references longer than UINT32_MAX
    h->sdict = long_refs;

    kh_destroy(s2i, d);

    if (str.l == 0)
        kputsn("", 0, &str);
    h->l_text = str.l;
    h->text = ks_release(&str);
    result = sam_hdr_sanitise(h);
    result->ref_count = 1;

    return result;

 error:
    if (h && d && (!h->target_name || !h->target_len)) {
        for (k = kh_begin(d); k != kh_end(d); ++k)
            if (kh_exist(d, k)) free((void *)kh_key(d, k));
    }
    sam_hdr_destroy(h);
    ks_free(&str);
    kh_destroy(s2i, d);
    kh_destroy(s2i, long_refs);
    if (sn) free(sn);
    return NULL;
}

int getline_stdin(htsFile *fp, int delimiter, kstring_t *s)
{
    int ret;

    // there will be a line feed (or a new line) before '\0', s->l represents the length of the total array, including '\0'
    ret = getline(&(s->s), &(s->l), stdin);
    if(ret == -1)
        return ret;
    
    s->m = s->l;
    s->l = strlen(s->s) - 1;
    s->s[s->l] = '\0';

    if (ret >= 0) ret = s->l;

    return ret;
}