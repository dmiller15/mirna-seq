#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import sqlalchemy
#
import bam_util
import picard_bam_sort
import bam_validate
import fastq_validate
import pipe_util


def is_dir(d):
    if os.path.isdir(d):
        return d
    raise argparse.ArgumentTypeError('%s is not a directory' % d)

def main():
    parser = argparse.ArgumentParser('miRNA harmonization')

    # Logging flag
    parser.add_argument('-d', '--debug',
                        action = 'store_const',
                        const = logging.DEBUG,
                        dest = 'level',
                        help = 'Enable debug logging.',
    )
    parser.set_defaults(level = logging.INFO)

    # Required flags
    parser.add_argument('-r', '--reference_fasta_path',
                        required = False,
                        help = 'Reference fasta path.',
    )
    parser.add_argument('-b', '--bam_path',
                        nargs = '?',
                        default = [sys.stdin],
                        help = 'Source bam path.',
    )
    parser.add_argument('-l', '--log_dir',
                        required = False,
                        type = is_dir,
                        help = 'Log file directory.',
    )
    parser.add_argument('-u', '--uuid',
                        required = False,
                        help = 'analysis_id string',
    )
    args = parser.parse_args()

    reference_fasta_path = args.reference_fasta_path
    preharmonized_bam_path = args.bam_path
    log_dir = args.log_dir

    uuid = args.uuid

    # Logging Setup
    logging.basicConfig(
        filename = os.path.join(log_dir, 'aln_' + uuid + '.log'),
        filemode = 'a',
        level = args.level,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d_%H:%M:%S_%Z',
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)
    logger.info('preharmonized_bam_path=%s' % preharmonized_bam_path)

    engine_path = 'sqlite:///' + os.path.join(log_dir, uuid + '_harmonize.db')
    engine = sqlalchemy.create_engine(engine_path, isolation_level='SERIALIZABLE')

    
    bam_validate.bam_validate(uuid, preharmonized_bam_path, engine, logger)
    readgroup_path_dict = bam_util.write_readgroups(uuid, preharmonized_bam_path, engine, logger)    
    bam_util.bam_to_fastq(uuid, preharmonized_bam_path, engine, logger)

    top_dir = os.path.dirname(preharmonized_bam_path)
    fastq_dir = os.path.join(top_dir, 'fastq')
    fastq_validate.fastq_guess_encoding(uuid, fastq_dir, engine, logger)
    
    fastq_validate.fastq_validate(uuid, fastq_dir, engine, logger)

    # Harmonization
    be_lenient = False
    harmonized_readgroup_bam_path_list = bam_util.bwa(uuid, preharmonized_bam_path, reference_fasta_path, readgroup_path_dict, engine, logger)
                             
    for harmonized_readgroup_bam_path in harmonized_readgroup_bam_path_list:
        if pipe_util.is_aln_bam(harmonized_readgroup_bam_path, logger):
            be_lenient = True
                
    harmonized_sorted_bam_path_list = picard_bam_sort.bam_sort(uuid, preharmonized_bam_path, harmonized_readgroup_bam_path_list, reference_fasta_path, engine, logger, be_lenient)

    for harmonized_sorted_bam_path in harmonized_sorted_bam_path_list:
        bam_validate.bam_validate(uuid, harmonized_sorted_bam_path, engine, logger)


if __name__ == '__main__':
    main()
