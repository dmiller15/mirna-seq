import os
import sys
#
import pandas as pd
#
import fastq_util
import time_util
import pipe_util

def picard_markduplicates_to_dict(uuid, bam_path, metrics_path, logger):
    data_dict = dict()
    read_header = False
    with open(metrics_path, 'r') as metrics_open:
        for line in metrics_open:
            if line.startswith("## HISTOGRAM"):
                break
            if line.startswith('#') or len(line) < 5:
                continue
            if not read_header:
                value_key_list = line.strip('\n').split('\t')
                logger.info('picard_markduplicates_to_dict() header value_key_list=\n\t%s' % value_key_list)
                logger.info('len(value_key_list=%s' % len(data_list))
                read_header = True
            else:
                data_list = line.strip('\n').split('\t')
                logger.info('picard_markduplicates_do_dict() data_list=\n\t%s' data_list)
                logger.info('len(data_list)=%s' % len(data_list))
                for value_pos, value_key in enumerate(value_key_list):
                    data_dict[value_key] = data_list[value_pos]
    logger.info('picard_markduplicates data_dict=%s' % data_dict)
    return data_dict

def bam_markduplicates(uuid, bam_path, fastq_dir, logger, be_lenient):
    sort_dir = os.path.dirname(bam_path)
    sort_parent_dir = os.path.dirname(sort_dir)
    md_dir = os.join.path(sort_parent_dir, 'md')
    os.makedirs(md_dir, exist_ok=True)
    logger.info('md_dir=%s' % md_dir)
    step_dir = md_dir
    outbam = os.path.basename(bam_path)
    outbam_path = os.path.join(md_dir, outbam)
    metrics_path = outbam_path + '.metrics'
    logger.info('outbam_path=%s' % outbam_path)

    # ignore already step for now

    
