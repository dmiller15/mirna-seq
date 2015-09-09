import os
import sys
#
import pipe_util
import time_util
import df_util

def bam_sort(uuid, preharmonized_bam_path, bam_path_list, reference_fasta_path, engine, logger, be_lenient):
    out_bam_path_list = list()
    for input_bam in bam_path_list:
        bam_name = os.path.basename(input_bam)
        bam_base, bam_ext = os.path.splitext(bam_name)
        input_dir = os.path.dirname(input_bam)
        outdir_path = os.path.join(input_dir, 'sorted')
        outbam_path = os.path.join(outdir_path, bam_name)
        tmpfile = os.path.join(outdir_path, 'tmpfile_' + bam_name)
        logger.info('outbam_path=%s' % outbam_path)
        out_bam_path_list.append(outbam_path)
        # already step left out
        logger.info('running step `picard sort` of: %s' % bam_name)
        os.makedirs(outdir_path, exist_ok=True)
        home_dir = os.path.expanduser('~')
        cmd = ['java', '-d64', '-jar', os.path.join(home_dir, 'tools/picard-tools/picard.jar'), 'SortSam', 'SORT_ORDER=coordinate', 'INPUT=' + input_bam, 'OUTPUT=' + outbam_path, 'TMP_DIR=' + outdir_path, 'CREATE_INDEX=true', 'REFERENCE_SEQUENCE=' + reference_fasta_path]
        if be_lenient:
            cmd.append('VALIDATION_STRINGENCY=LENIENT')
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['bam_path'] = outbam_path
        df['reference_fasta_path'] = reference_fasta_path
        unique_key_dict = {'uuid': uuid, 'bam_path': outbam_path, 'reference_fasta_path': reference_fasta_path}
        table_name = 'time_mem_picard_bamsort'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        logger.info('completed running step `picard sort` of: %s' % bam_name)
    return out_bam_path_list
        
