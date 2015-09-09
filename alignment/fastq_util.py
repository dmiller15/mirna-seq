import glob
import os
import re
import subprocess
import sys

import pipe_util

def buildsefastqlist(fastqlist):
    sefastq_list = list()
    fastq_re = re.compile('(^[A-Z0-9_.-]+)_(s).fq')
    for fastq in fastqlist:
        fastq_match = fastq_re.match(fastq)
        if fastq_match is None:
            continue
        if len(fastq_match.groups()) == 2:
            seread = fastq_match.group()
            sefastq_list.append(seread)
    return sefastq_list

def buildfastqlist(adir, logger):
    sorted_fastqlist_fiile=os.path.join(adir, 'fastqlist.txt')
    # Already step omitted
    logger.info('building fastq list in %s' % adir)
    fastqlist = [os.path.basename(fastq) for fastq in (glob.glob(os.path.join(adir, '*.fq')))]
    sorted_fastqlist = sorted(fastqlist)
    with open(sorted_fastqlist_file, 'w') as sorted_fastqlist_open:
        sorted_fastqlist_open.write(str(sorted_fastqlist))
    # Create already step omitted
    logger.info('completed building fastq list in %s' % adir)
    return sorted_fastqlist

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
