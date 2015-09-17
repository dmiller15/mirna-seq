import os
import sys

import pandas
import sqlite3


def get_flagstat_bam_total_read_count(uuid, bam_path, engine, logger):
    df = pandas.read_sql_query('select * from samtools_flagstat where bam_path = "' + bam_path + '"' + \
                               ' and uuid="' + uuid + '"', engine)
    assert(len(df) == 1) # should be 1 unique result gdc-wide
    paired_in_sequencing = int(df['paired in sequencing'].values[0])
    read1 = int(df['read1'].values[0])
    read2 = int(df['read2'].values[0])
    if (read1 + read2 == paired_in_sequencing):
        logger.info('`samtools flagstat` read1 + read2 == paired_in_sequencing')
        logger.info(str(read1) + ' + ' + str(read2) + ' == ' + str(paired_in_sequencing))
        return paired_in_sequencing
    else:
        logger.info('`samtools flagstat` read1 + read2 != paired_in_sequencing')
        logger.info(str(read1) + ' + ' + str(read2) + ' != ' + str(paired_in_sequencing))
        logger.info('exiting pipeline')
        sys.exit(1)
        

def get_fastqc_total_read_count(uuid, fastq_list, fastq_dir, engine, logger):
    fastq_read_count_dict = dict()
    for fastq_name in fastq_list:
        fastq_path = os.path.join(fastq_dir, fastq_name)
        df = pandas.read_sql_query('select * from fastqc_data_Basic_Statistics where fastq_path = "' + fastq_path +
                                   '" and Measure="Total Sequences" and uuid="' + uuid + '"', engine)
        assert(len(df) == 1) # should be 1 unique result gdc-wide
        total_sequences = int(df['Value'].values[0])
        fastq_read_count_dict[fastq_name] = total_sequences
    fastq_read_count = sum(fastq_read_count_dict.values())
    return fastq_read_count

def final_validate(uuid, preharmonized_bam_path, harmonized_bam_md_path, fastq_list, fastq_dir, engine, logger):
    preharmonized_total_read_count = get_flagstat_bam_total_read_count(uuid, preharmonized_bam_path, engine, logger)
    harmonized_total_read_count = get_flagstat_bam_total_read_count(uuid, harmonized_bam_md_path, engine, logger)
    fastq_read_count = get_fastqc_total_read_count(uuid, fastq_list, fastq_dir, engine, logger)
    if (preharmonized_total_read_count == harmonized_total_read_count == fastq_read_count):
        logger.info('SUCCESS')
        logger.info('preharmonized_total_read_count == harmonized_total_read_count == fastq_read_count')
        logger.info(str(preharmonized_total_read_count) + ' == ' + str(harmonized_total_read_count) + ' == ' + str(fastq_read_count))
        return
    else:
        logger.info('preharmonized_total_read_count != harmonized_total_read_count != fastq_read_count')
        logger.info(str(preharmonized_total_read_count) + ' != ' + str(harmonized_total_read_count) + ' != ' + str(fastq_read_count))
        logger.info('exiting pipeline')
        sys.exit(1)
        
