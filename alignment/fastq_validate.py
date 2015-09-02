import logging
import os
import shlex
import subprocess
import sys
#
import pandas as pd
#
import pipe_util
import fastq_util

def do_fastqc(uuid, fastq_path, logger):
    fastq_name = os.path.basename(fastq_path)
    fastq_dir = os.path.dirname(fastq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    # already step
    logger.info('running step `fastqc`: %s' % fastq_path)
    cmd = ['fastqc', '--extract', fastq_path]
    output = pipe_util.do_command(cmd, logger)
    # create already step
    return

def get_fastq_size(fq_path, logger):
    fq_dir = os.path.dirname(fq_path)
    fq_name = os.path.basename(fq_path)
    size_file = os.path.join(fq_dir, fq_name + '.size')
    # already step
    logger.info('determining size of fq %s' % fq_name)
    size_value = os.path.getsize(fq_path)
    with open(size_file, 'w') as size_open:
        size_open.write(str(size_value))
    # create already step
    logger.info('determined size of fq %s: %s' % (fq_name, str(size_value)))
    return size_value

def fastq_validate(uuid, fq_path, logger):
    fq_path_size = get_fastq_size(fq_path, logger)
    if fq_path_size > 0:
        do_fastqc(uuid, fq_path, logger)
