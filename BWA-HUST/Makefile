CC=			g++
#CC=			clang --analyze
CFLAGS=		-g -Wall -Wno-unused-function  -march=haswell -O3
#-fsanitize=address -fno-omit-frame-pointer
WRAP_MALLOC=-DUSE_MALLOC_WRAPPERS
AR=			ar
DFLAGS=		-DHAVE_PTHREAD $(WRAP_MALLOC)

MEM_FLAGS=	-DSAIS=1 -std=c++17
CPPFLAGS+=	-g -O3 -DENABLE_PREFETCH -DV17=1 -DMATE_SORT=0 $(MEM_FLAGS) -mpopcnt
#-fsanitize=address -fno-omit-frame-pointer
LISA_SMEM_FLAGS= -DOUTPUT -DNO_DNA_ORD -DREV_COMP -DBWA_MEM_BUG -DNOCHUNK -DVECTORIZE  -DENABLE_PREFETCH -D_64BIT -DHUGE_PAGE_  -DLINEAR_ONLY_ # flag ending with '_' are disabled flags, remove '_' to enable. Example: To enable HUGE_PAGE, " -DHUGE_PAGE_ -> -DHUGE_PAGE"

#ifeq ($(linear_only),1)
	LISA_SMEM_FLAGS += -DLINEAR_ONLY
#endif

CFLAGS+= $(LISA_SMEM_FLAGS)
CPPFLAGS+= $(LISA_SMEM_FLAGS)
EXT=          ./ext/TAL
EXTEXT=       $(EXT)/ext
EXTBSW=      $(EXT)/src/BSW
EXTFMI=      $(EXT)/src/FMI
EXTLISASRC=  $(EXT)/src/LISA-FMI

LOBJS=		utils.o kthread.o kstring.o ksw.o bwt.o bntseq.o bwa.o bwamem.o bwamem_pair.o bwamem_extra.o malloc_wrap.o \
			QSufSort.o rope.o rle.o is.o bwt_gen.o  $(EXTFMI)/FMI_search.o bwtindex.o
LISA_OBJ=	$(EXTLISASRC)/thread_data.o $(EXTLISASRC)/lisa_util.o
LOBJS+= $(LISA_OBJ)

AOBJS=		 bwaseqio.o  \
			 kopen.o  \
			fastmap.o
PROG=		BWA-HUST
INCLUDES=	
LIBS=		-lm -lz -lpthread
SUBDIRS=	.


INCLUDES=   -I$(EXT)/ext/safestringlib/include -I$(EXTBSW) -I$(EXTFMI) -I$(EXTEXT) -I$(EXTLISASRC) -I.
LIBS=		-lm -lz -lpthread -L$(EXT)/ext/safestringlib -lsafestring $(STATIC_GCC)
#OBJS=		src/fastmap.o src/bwtindex.o $(EXTEXT)/utils.o src/memcpy_bwamem.o src/kthread.o \
#			src/kstring.o src/ksw.o $(EXTEXT)/bntseq.o src/bwamem.o src/profiling.o $(EXTBSW)/bandedSWA.o \
#			$(EXTFMI)/FMI_search.o  src/bwamem_pair.o src/kswv.o src/bwa.o $(EXTEXT)/bseq.o\
#			src/bwamem_extra.o src/kopen.o
LISA_OBJ=	$(EXTLISASRC)/thread_data.o $(EXTLISASRC)/lisa_util.o



ifeq ($(shell uname -s),Linux)
	LIBS += -lrt
endif

.SUFFIXES:.c .o .cc

.c.o:
		$(CC) -c $(CFLAGS) $(DFLAGS) $(INCLUDES) $< -o $@
.cpp.o:
		$(CXX) -c $(CPPFLAGS) $(DFLAGS) $(INCLUDES) $< -o $@

all:$(PROG)

BWA-HUST:libbwa.a $(AOBJS) main.o
		cd $(EXT)/ext/safestringlib/ && $(MAKE)
		cd ../../../../
		$(CC) $(CFLAGS) $(CPPFLAGS) $(DFLAGS) $(AOBJS) main.o -L. -lbwa $(LIBS)  -o $@

libbwa.a:$(LOBJS)
		$(AR) -csru $@ $(LOBJS)

clean:
		rm -f gmon.out *.o a.out $(PROG) *~ *.a
		cd $(EXT)/ext/safestringlib/ && $(MAKE) clean
		rm -rf $(EXTBSW)/*.o
		rm -rf $(EXTFMI)/*.o
		rm -rf $(EXTEXT)/*.o
		rm -rf $(EXTLISASRC)/*.o

depend:
	( LC_ALL=C ; export LC_ALL; makedepend -Y -- $(CFLAGS) $(DFLAGS) -- *.c )

# DO NOT DELETE THIS LINE -- make depend depends on it.

$(EXTFMI)/FMI_search.o: $(EXTFMI)/FMI_search.h $(EXTEXT)/bntseq.h
$(EXTFMI)/FMI_search.o: $(EXTEXT)/sais.h bwa.h $(EXTEXT)/bseq.h
QSufSort.o: QSufSort.h
bntseq.o: bntseq.h utils.h kseq.h malloc_wrap.h khash.h
bwa.o: bntseq.h bwa.h bwt.h ksw.h utils.h kstring.h malloc_wrap.h kvec.h kseq.h
bwamem.o: kstring.h malloc_wrap.h bwamem.h bwt.h bntseq.h bwa.h ksw.h kvec.h
bwamem.o: ksort.h utils.h kbtree.h $(EXTFMI)/FMI_search.h $(EXTLISASRC)/LISA_search.h
bwamem_extra.o: bwa.h bntseq.h bwt.h bwamem.h kstring.h malloc_wrap.h
bwamem_pair.o: kstring.h malloc_wrap.h bwamem.h bwt.h bntseq.h bwa.h kvec.h
bwamem_pair.o: utils.h ksw.h
bwaseqio.o: bwt.h utils.h malloc_wrap.h kseq.h
bwt.o: utils.h bwt.h kvec.h malloc_wrap.h
bwt_gen.o: QSufSort.h malloc_wrap.h
bwtindex.o: bntseq.h bwa.h bwt.h utils.h rle.h rope.h malloc_wrap.h
fastmap.o: bwa.h bntseq.h bwt.h bwamem.h kvec.h malloc_wrap.h utils.h kseq.h
is.o: malloc_wrap.h
kopen.o: malloc_wrap.h
kstring.o: kstring.h malloc_wrap.h
ksw.o: ksw.h malloc_wrap.h
main.o: kstring.h malloc_wrap.h utils.h
malloc_wrap.o: malloc_wrap.h
rle.o: rle.h
rope.o: rle.h rope.h
utils.o: utils.h ksort.h malloc_wrap.h kseq.h
