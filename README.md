# BWA-HUST
BWA-HUST is an optimized version of the bwa-mem algorithm in bwa.

## Building BWA-HUST
build the code from a Git repository requires following steps:
```sh
git clone https://github.com/536260668/fast-genomic-data-preprocessing
cd fast-genomic-data-preprocessing/BWA-HUST/
make
```
## Running BWA-HUST
### Usage
```sh
BWA-HUST <command> [options]
```
### Command
```sh
index         index sequences in the FASTA format
mem           BWA-MEM algorithm
fastmap       identify super-maximal exact matches
pemerge       merge overlapping paired ends (EXPERIMENTAL)
aln           gapped/ungapped alignment
samse         generate alignment (single ended)
sampe         generate alignment (paired ended)
bwasw         BWA-SW for long queries
shm           manage indices in shared memory
fa2pac        convert FASTA to PAC format
pac2bwt       generate BWT from PAC
pac2bwtgen    alternative algorithm for generating BWT
bwtupdate     update .bwt to the new format
bwt2sa        generate SA from BWT and Occ
```
### Note
To use BWA, you need to first index the genome with `BWA-HUST index`.

There are three alignment algorithms in BWA: `mem`, `bwasw`, and`aln/samse/sampe`. If you are not sure which to use, try `BWA-HUST mem` first.

### Example
```sh
  BWA-HUST mem ref.fa reads.fq > aln.sam
  BWA-HUST aln ref.fa reads.fq > reads.sai;
  BWA-HUST samse ref.fa reads.sai reads.fq > aln-se.sam
  BWA-HUST mem ref.fa read1.fq read2.fq > aln-pe.sam
  BWA-HUST aln ref.fa read1.fq > read1.sai;
  BWA-HUST aln ref.fa read2.fq > read2.sai
  BWA-HUST sampe ref.fa read1.sai read2.sai read1.fq read2.fq > aln-pe.sam
```

# SortMarkDup
The typical output produced by BWA-MEM is unordered and contains many duplicate reads, requiring tools for sorting and marking PCR and optical duplicates. SortMarkDup is a new algorithm that reorganizes the procedure of sorting and marking duplicates to avoid intensive and time-consuming I/O.

## Building SortMarkDup
build the code from a Git repository requires following steps:
```sh
git clone https://github.com/536260668/fast-genomic-data-preprocessing
cd fast-genomic-data-preprocessing/SortMarkDup/
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_MAKE_PROGRAM=ninja -G Ninja -S /your/path/to/SortMarkDup -B /your/path/to/SortMarkDup/release
cmake --build /your/path/to/SortMarkDup/release -j <threads>
```
## Running SortMarkDup
```sh
 ./tbb-sormadup in.sam out.bam
```
