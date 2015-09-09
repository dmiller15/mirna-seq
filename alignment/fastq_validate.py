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
import df_util
import time_util

def guess_enc_db(uuid, fq_path, engine, logger):
    fastq_dir = os.path.dirname(fq_path)
    fastq_name = os.path.basename(fq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    guess_enc_path = fq_path + '.format'
    guess_enc_value = str()
    with open(guess_enc_path, 'r') as guess_enc_open:
        guess_enc_value = guess_enc_open.readline().strip()
    data_dict = dict()
    # already step omitted
    logger.info('writing `guess_enc_db`: %s' fq_path)
    data_dict['uuid'] = [uuid]
    data_dict['fastq_name'] = fastq_name
    data_dict['guess'] = guess_enc_value
    df = pd.DataFrame(data_dict)
    table_name = 'guess_fastq_encoding'
    unique_key_dict = {'uuid': uuid, 'fastq_name': fastq_name}
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    # create already step omitted
    logger.info('completed writing `guess_enc_db`: %s' % fq_path)

def do_fastqc(uuid, fastq_path, logger):
    fastq_name = os.path.basename(fastq_path)
    fastq_dir = os.path.dirname(fastq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    # already step
    logger.info('running step `fastqc`: %s' % fastq_path)
    cmd = ['/home/ubuntu/tools/FastQC/fastqc', '--extract', fastq_path]
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

def fastq_guess_encoding(uuid, fastq_dir, engine, logger): # future thread count
    logger.info('enter fastq_guess_encoding()')
    fastqlist = fastq_util.buildfastqlist(fastq_dir, logger)
    logging.info('fastqlist=%s' % fastqlist)
    sefastqlist = fastq_util.buildsefastqlist(fastqlist)
    logger.info('sefastqlist=%s' % sefastqlist)
    for seread in sefastqlist:
        fq_path = os.path.join(fq_path, logger)
        fq_path_size = get_fastq_size(fq_path, logger)
        if fq_path_size > 0:
            do_guess_encoding(uuid, fq_path, engine, logger)
            guess_enc_db(uuid, fq_path, engine, logger)
            

def fastq_validate(uuid, fq_path, logger):
    fq_path_size = get_fastq_size(fq_path, logger)
    if fq_path_size > 0:
        do_fastqc(uuid, fq_path, logger)

def write_fastq_format(fastq_path, output, logger):
    outfile = fastq_path + '.format'
    # need to read last txt line instead (2nd to last line)
    output_list = output.decode().split('\n')
    logger.info('fastq guess list: %s' % output_list)
    outstring = output_list[-2].strip()
    if outstring == 'Exit status: 0': # handle awk terminating prematurely when guess happens before EOF
        outfile = output_list[1].strip().split('\t')[0]
    outfile_open = open(outfile, 'w')
    outfile_open.write(outstring)
    outfile_open.close()
    if outstring == 'Illumina-1.8':
        logger.info('write_fastq_format() guessed encoding: %s' % outstring)
        return

def do_guess_encoding(uuid, fastq_path, engine, logger):
    fastq_name = os.path.basename(fastq_path)
    fastq_dir = os.path.dirname(fastq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    # already step omitted
    logger.info('running step `guess encoding` of %s' % fastq_path)
    pipe_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    guess_path = os.path.join(pipe_dir, 'guess-encoding.py')
    guess_cmd = 'python2' + guess_path
    proc = subprocess.Popen(time_cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    output = proc.communicate()[0]
    logger.info('output=%s' % output)
    df = time_util.store_time(uuid, time_cmd, output, logger)
    df['fastq_path'] = fastq_path
    table_name = 'time_mem_guessencoding'
    unique_key_dict = {'uuid': uuid, 'fastq_path': fastq_path}
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    logger.info('do_guess_encoding output=%s' % output.decode())
    write_fastq_format(fastq_path, output, logger)
    # create already step omitted
