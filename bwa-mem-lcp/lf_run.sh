BIN=/home/cluster/Storage/bin
reference=/home/cluster/Storage/reference_sequence/hs37d5.fasta
wgs_tumor1=/home/cluster/Storage/fastq/wgs/merge_data/case/case_1.fastq.gz
wgs_tumor2=/home/cluster/Storage/fastq/wgs/merge_data/case/case_2.fastq.gz
wgs_normal1=/home/cluster/Storage/fastq/wgs/merge_data/control/bgz/control_1.fastq.gz
wgs_normal2=/home/cluster/Storage/fastq/wgs/merge_data/control/bgz/control_2.fastq.gz
ts_tumor1=/home/cluster/Storage/fastq/ts/merge_data/case/case_1.fastq.gz
ts_tumor2=/home/cluster/Storage/fastq/ts/merge_data/case/case_2.fastq.gz
ts_normal1=/home/cluster/Storage/fastq/ts/merge_data/control/control_1.fastq.gz
ts_normal2=/home/cluster/Storage/fastq/ts/merge_data/control/control_2.fastq.gz
wes_tumor1=/home/cluster/Storage/fastq/wes/case/bgz/case_1.fastq.gz
wes_tumor2=/home/cluster/Storage/fastq/wes/case/bgz/case_2.fastq.gz
wes_normal1=/home/cluster/Storage/fastq/wes/control/control_1.fastq.gz
wes_normal2=/home/cluster/Storage/fastq/wes/control/control_2.fastq.gz
ref2=/home/cluster/Storage/lambert-ref-index2/hs37d5.fasta

# speed up memory allocation malloc in bwa
export LD_PRELOAD=/home/cluster/Storage/bin/sentieon-genomics-202010/lib/libjemalloc.so

echo "===============test bwa-lfalive on ts_tumor dataset==============="
export MALLOC_CONF=background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always
echo "background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always"
(time ./bwa mem -M -t 55 -R "@RG\tID:tumor\tSM:tumor\tLB:tumorLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $ts_tumor1 $ts_tumor2 >/dev/null) 2>bwa-lfalive_ts_tumor.log

echo "===============test bwa-lfalive on ts_normal dataset==============="
export MALLOC_CONF=background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always
echo "background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always"
(time ./bwa mem -M -t 55 -R "@RG\tID:normal\tSM:normal\tLB:normalLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $ts_normal1 $ts_normal2 >/dev/null) 2>bwa-lfalive_ts_normal.log

echo "===============test bwa-lfalive on wes_tumor dataset==============="
export MALLOC_CONF=background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always
echo "background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always"
(time ./bwa mem -M -t 55 -R "@RG\tID:tumor\tSM:tumor\tLB:tumorLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $wes_tumor1 $wes_tumor2 >/dev/null) 2>bwa-lfalive_wes_tumor.log

echo "===============test bwa-lfalive on wes_normal dataset==============="
export MALLOC_CONF=background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always
echo "background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always"
(time ./bwa mem -M -t 55 -R "@RG\tID:normal\tSM:normal\tLB:normalLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $wes_normal1 $wes_normal2 >/dev/null) 2>bwa-lfalive_wes_normal.log

echo "===============test bwa-lfalive on wgs_tumor dataset==============="
export MALLOC_CONF=background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16
echo "background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16"
(time ./bwa mem -M -t 55 -R "@RG\tID:tumor\tSM:tumor\tLB:tumorLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $wgs_tumor1 $wgs_tumor2 >/dev/null) 2>bwa-lfalive_wgs_tumor.log

echo "===============test bwa-lfalive on wgs_normal dataset==============="
export MALLOC_CONF=background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16
echo "background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16"
(time ./bwa mem -M -t 55 -R "@RG\tID:normal\tSM:normal\tLB:normalLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $wgs_normal1 $wgs_normal2 >/dev/null) 2>bwa-lfalive_wgs_normal.log