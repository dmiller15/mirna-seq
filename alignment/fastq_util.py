import glob
import os
import re
import subprocess
import sys

import pipe_util


def buildfastq_len_list(adir):
    fastq_len_list = [fastq_path for fastq_path in (glob.glob(os.path.join(adir, '*.fq.len')))]
    return sorted(fastq_len_list)

def get_maxofmax_fastq_length(fastq_dir, logger):
    fastq_len_list = buildfastq_len_list(fastq_dir)
    max_len = 0
    for fastq_len_file in fastq_len_list:
        with open(fastq_len_file, 'r') as fastq_len_file_open:
            logger.info('fastq_len_file=%s' % fastq_len_file)
            fastq_len_str = fastq_len_file_open.readline().strip()
            if fastq_len_str == '':
                continue
            fastq_len = int(fastq_len_str)
            logger.info('\t fastq_len=%s' % str(fastq_len))
            if fastq_len > max_len:
                max_len = fastq_len
    logger.info('max_len=%s' % str(max_len))
    return max_len
