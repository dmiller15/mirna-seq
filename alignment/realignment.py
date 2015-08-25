#!/usr/bin/env python3

import argparse
import logging
import os
import sys

#import setupLog
import bam_util

def is_dir(d):
    if os.path.isdir(d):
        return d
    raise argparse.ArgumentTypeError('%s is not a directory' % d)

def main():
    parser = argparse.ArgumentParser('miRNA harmonization')
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
        level = logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d_%H:%M:%S_%Z',
    )
    logging.getLogger(uuid).setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)
    logger.info('preharmonized_bam_path=%s' % preharmonized_bam_path)

    bam_util.new_bam_to_fastq(uuid, preharmonized_bam_path, logger)

    top_dir = os.path.dirname(preharmonized_bam_path)
    fastq_dir = os.path.join(top_dir, 'fastq')

    for filename in os.listdir(fastq_dir):
        output = bam_util.new_bwa_aln_single(uuid, top_dir, fastq_dir, filename, top_dir, reference_fasta_path, logger)


if __name__ == '__main__':
    main()
