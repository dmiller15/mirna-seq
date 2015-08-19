import glob
import os
import logging
import sys
#
import bam_validate
import df_util
import pipe_util
import time_util
#
import pysam
import pandas as pd


def bam_to_fastq(uuid, bam_path, engine, logger):
    step_dir = os.path.dirname(bam_path)
    uuid_dir = step_dir

    logger.info('uuid_dir is: %s' % uuid_dir)
    fastq_dir = os.path.join(uuid_dir, 'fastq')
    logger.info('fastq_dir is: %s' % fastq_dir)
    if pipe_util.already_step(fastq_dir, 'fastq', logger):
        logger.info('already completed step `bamtofastq` of: %s' % bam_path)
    else:
        logger.info('running step `bamtofastq` of %s: ' % bam_path)
        os.makedirs(fastq_dir, exist_ok=True)
        tempfq = os.path.join(fastq_dir, 'tempfq')
        cmd = ['bamtofastq', 'filename=' + bam_path, 'outputdir=' + fastq_dir, 'tryoq=1', 'collate=1', 'outputperreadgroup=1', 'T=' + tempfq]
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['bam_path'] = bam_path
        unique_key_dict = {'uuid': uuid, 'bam_path': bam_path}
        table_name = 'time_mem_bamtofastq'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(fastq_dir, 'fastq', logger)
        logger.info('completed running step `bamtofastq` of: %s' % bam_path)
    return
