import os
import sys
#
import pipe_util

def bam_sort(uuid, preharmonized_bam_path, harmonized_bam_path, reference_fasta_path, logger, be_lenient):
    input_bam = harmonized_bam_path
    bam_name = os.path.basename(harmonized_bam_path)
    bam_base, bam_ext = os.path.splitext(bam_name)
    input_dir = os.path.dirname(harmonized_bam_path)
    outdir_path = os.path.join(input_dir, 'sorted')
    outbam_path = os.path.join(outdir_path, bam_name)
    tmpfile = os.path.join(outdir_path, 'tmpfile_' + bam_name)
    logger.info('outbam_path=%s' % outbam_path)
    # already step left out
    os.makedirs(outdir_path, exist_ok=True)
    home_dir = os.path.expanduser('~')
    cmd = ['java', '-d64', '-jar', os.path.join(home_dir, 'tools/picard-tools/picard.jar'), 'SortSam', 'SORT_ORDER=coordinate', 'INPUT=' + input_bam, 'OUTPUT=' + outbam_path, 'TMP_DIR=' + outdir_path, 'CREATE_INDEX=true', 'REFERENCE_SEQUENCE=' + reference_fasta_path]
    if be_lenient:
        cmd.append('VALIDATION_STRINGENCY=LENIENT')
    output = pipe_util.do_command(cmd, logger)
    logger.info('completed running step `picard sort` of: %s' % bam_name)
    return outbam_path
        
