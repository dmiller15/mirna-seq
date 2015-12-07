#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import sqlalchemy
import subprocess
import pandas as pd
#



#

def is_dir(d):
    if os.path.isdir(d):
        return d
    raise argparse.ArgumentTypeError('%s is not a directory' %d)


def do_command(cmd, logger, stdout=subprocess.STDOUT, stderr=subprocess.PIPE, allow_fail=False):
    #env = update_env(logger)
    timecmd = cmd
    timecmd.insert(0, '/usr/bin/time')
    timecmd.insert(1, '-v')
    logger.info('running cmd: %s' % timecmd)
    try:
        output = subprocess.check_output(timecmd, stderr=subprocess.STDOUT)
        logger.info('contents of output(s)=%s' % output.decode().format())
    except Exception as e:
        logger.debug('failed cmd: %s' % str(timecmd))
        logger.debug('exception: %s' % e)
        if allow_fail:
            return e.output
        else:
            sys.exit('failed cmd: %s' % str(timecmd))
    logger.info('completed cmd: %s' % str(timecmd))
    return output


def do_shell_command(cmd, logger, stdout=subprocess.STDOUT, stderr=subprocess.PIPE):
    timecmd = '/usr/bin/time -v ' + cmd
    logger.info('running cmd: %s' % timecmd)
    try:
        output = subprocess.check_output(timecmd, stderr=subprocess.STDOUT, shell=True)
        logger.info('contents of output(s)=%s' % output.decode().format())
    except Exception as e:
        logger.debug('failed cmd: %s' % str(timecmd))
        logger.debug(e.output)
        logger.debug('exception: %s' % e)
        sys.exit('failed cmd: %s' % str(timecmd))
    logger.info('completed cmd: %s' % str(timecmd))
    return output

def store_time(uuid, cmd, output, logger):
    user_time = float()
    system_time = float()
    percent_of_cpu = int()
    wall_clock = float()
    maximum_resident_set_size = int()
    exit_status = int()
    for line in output.decode().format().split('\n'):
        line = line.strip()
        if line.startswith('User time (seconds):'):
            user_time = float(line.split(':')[1].strip())
        if line.startswith('System time (seconds):'):
            system_time = float(line.split(':')[1].strip())
        if line.startswith('Percent of CPU this job got:'):
            percent_of_cpu = int(line.split(':')[1].strip().rstrip('%'))
            assert (percent_of_cpu is not 0)
        if line.startswith('Elapsed (wall clock) time (h:mm:ss or m:ss):'):
            value = line.replace('Elapsed (wall clock) time (h:mm:ss) or m:ss:', '').strip()
            # hour case
            if value.count(':') == 2:
                hours = int(value.split(':')[0])
                minutes = int(value.split(':')[1])
                seconds = float(value.split(':')[2])
                total_seconds = (hours * 60 * 60) + (minutes * 60) + seconds
                wall_clock = total_seconds
            # under hour case
            if value.count(':') == 1:
                minutes = int(value.split(':')[0])
                seconds = float(value.split(':')[1])
                total_seconds = (minutes * 60) + seconds
                wall_clock = total_seconds
        if line.startswith('Maximum resident set size (kbytes):'):
            maximum_resident_set_size = int(line.split(':')[1].strip())
        if line.startswith('Exit status:'):
            exit_status = int(line.split(':')[1].strip())

    df = pd.DataFrame({'uuid': [uuid],
                       'user_time': user_time,
                       'system_time': system_time,
                       'percent_of_cpu': percent_of_cpu,
                       'wall_clock': wall_clock,
                       'maximum_resident_set_size': maximum_resident_set_size,
                       'exit_status': exit_status})
    return df

def main():
    parser = argparse.ArgumentParser('miRNA profiling',
                                     description = 'The BCGSC miRNA Profiling Pipeline produces expression profiles of known miRNAs from BWA-aligned BAM files and generates summary reports and graphs describing the results.',
    )

    # Logging flag
    parser.add_argument('-d', '--debug',
                        action = 'store_const',
                        const = logging.DEBUG,
                        dest = 'level',
                        help = 'Enable debug logging.',
    )
    parser.set_defaults(level = logging.INFO)

    # Required flags
    parser.add_argument('-m', '--mirbase_db',
                        required = True,
                        choices = ['mirna_current'],
                        help = 'Name of desired miRbase.',
    )
    parser.add_argument('-u', '--ucsc_db',
                        required = True,
                        choices = ['hg38'],
                        help = 'Name of desired UCSC database.',
    )
    parser.add_argument('-o', '--species_code',
                        required = True,
                        choices = ['hsa'],
                        help = 'Organism species code.',
    )
    parser.add_argument('-p', '--project_dir',
                        required = True,
                        type = is_dir,
                        help = 'Path to directory containing bams.',
    )
    args = parser.parse_args()

    mirbase_db = args.mirbase_db
    ucsc_db = args.ucsc_db
    species_code = args.species_code
    project_dir = args.project_dir

    # Logging Setup
    logging.basicConfig(
        filename = os.path.join(project_dir, 'profiling.log'),
        filemode = 'a',
        level = args.level,
        format = '%(asctime)s %(levelname)s %(message)s',
        datefmt = '%Y-%m-%d_%H:%M:%S_%Z',
    )
    logging.getLogger('sqlalchemp.engine').setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)
    logger.info('project_dir=%s' % project_dir)

    engine_path = 'sqlite:///' + os.path.join(project_dir, 'profiling.db')
    engine = sqlalchemy.create_engine(engine_path, isolation_level='SERIALIZABLE')

    # Generate list of BAMs in project directory and its subdirectories
    bam_path_list = []
    for root, dirs, files in os.walk(project_dir):
        for name in files:
            if name.endswith(".bam"):
                bam_path_list.append(os.path.join(root,name))

    # Convert the BAMs to SAMs if they do not already exist
    logger.info('Beginning: BAM to SAM conversion')
    for bam_path in bam_path_list:
        bam_name, bam_ext = os.path.splitext(bam_path)
        sam_path = bam_name + '.sam'
        if os.path.exists(sam_path):
            logger.info('SAM file: %s already exists BAM path: %s' % (sam_path, bam_path))
        else:
            BAMtoSAM_CMD = ['samtools', 'view', '-h', bam_path, '-o', sam_path]
            shell_BtS_CMD = ' '.join(BAMtoSAM_CMD)
            do_shell_command(shell_BtS_CMD, logger)
            logger.info('SAM file: %s created for BAM file: %s' % (sam_path, bam_path))
    logger.info('Completed: BAM to SAM conversion')
    
    # Generate Adapter report if one does not already exist
    logger.info('Beginning: Adapter report generation')
    for bam_path in bam_path_list:
        bam_name, bam_ext = os.path.splitext(bam_path)
        report_path = bam_name + '_adapter.report'
        if os.path.exists(report_path):
            logger.info('Adapter report: %s for BAM file: %s already exists' % (report_path, bam_path))
        else:
            adapter_CMD = ["samtools", "view", bam_path, "|", "awk '{arr[length($10)]+=1} END {for (i in arr) {print i\" \"arr[i]}}'", "|", "sort -t \" \" -k1n >", report_path]
            shell_adapter_CMD = ' '.join(adapter_CMD)
            do_shell_command(shell_adapter_CMD, logger)
            logger.info('Adapter report: %s created for BAM file: %s' % (report_path, bam_path))
    logger.info('Completed: Adapter report generation')

    # Annotate the SAM files
    logger.info('Beginning: SAM file annotation')
    annotate_CMD = ['perl', '/home/ubuntu/bin/mirna/v0.2.7/code/annotation/annotate.pl', '-m', mirbase_db, '-u', ucsc_db, '-o', species_code, '-p', project_dir]
    do_command(annotate_CMD, logger)
    # Store time command will go here
    logger.info('Completed: SAM file annotation')

    # Get stats from the alignment annotations
    logger.info('Beginning: Alignment stats generation')
    stats_CMD = ['perl', '/home/ubuntu/bin/mirna/v0.2.7/code/library_stats/alignment_stats.pl', '-p', project_dir]
    do_command(stats_CMD, logger)
    # Store time command will go here
    logger.info('Completed: Alignment stats generation')

    # Generate TCGA formatted results
    logger.info('Beginning: TCGA formatted results generation')
    tcga_CMD = ['perl', '/home/ubuntu/bin/mirna/v0.2.7/code/custom_output/tcga/tcga.pl', '-m', mirbase_db, '-o', species_code, '-g', ucsc_db, '-p', project_dir]
    do_command(tcga_CMD, logger)
    # Store time command will go here
    logger.info('Completed: TCGA formatted results generation')
    
    # Generate the expression matrices for pre-miRNA
    logger.info('Beginning: Pre-miRNA gene expression matrix generation')
    matrix_CMD = ['perl', '/home/ubuntu/bin/mirna/v0.2.7/code/library_stats/expression_matrix.pl', '-m', mirbase_db, '-o', species_code, '-p', project_dir]
    do_command(matrix_CMD, logger)
    # Store time commmand will go here
    logger.info('Completed: Pre-miRNA gene expression matrix generation')

    # Generate the expression matrices for mature miRNA
    logger.info('Beginning: Mature miRNA gene expression matrix genreation')
    mimat_CMD = ['perl', '/home/ubuntu/bin/mirna/v0.2.7/code/library_stats/expression_matrix_mimat.pl', '-m', mirbase_db, '-o', species_code, '-p', project_dir]
    do_command(mimat_CMD, logger)
    # Store time command will go here
    logger.info('Completed: Mature miRNA gene expression matrix generation')

    # Generate the graphs for the annotation data
    logger.info('Beginning: Annotation graph generation')
    graph_CMD = ['perl', '/home/ubuntu/bin/mirna/v0.2.7/code/library_stats/graph_libs.pl', '-p', project_dir]
    do_command(graph_CMD, logger)
    # Store time command will go here
    logger.info('Completed: Annotation graph generation')



if __name__ == '__main__':
    main()
                          
