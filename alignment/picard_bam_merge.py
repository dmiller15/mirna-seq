import os
import sys
#
import df_util
import pipe_util
import time_util


def bam_merge(uuid, preharmonize_bam_path, bam_path_list, engine, logger, be_lenient):
    sorted_bam_dir = os.path.dirname(bam_path_list[0])
    bwa_alignment_dir = os.path.dirname(sorted_bam_dir)
    realn_dir = os.path.dirname(bwa_alignment_dir)
    out_dir = os.path.join(realn_dir, 'merge')
    os.makedirs(out_dir, exist_ok=True)
    step_dir = out_dir
    outbam_name = os.path.basename(preharmonize_bam_path)
    outbam_path = os.path.join(out_dir, outbam_name)
    logger.info('bam_path_list=%s' % bam_path_list)
    lenient_merge = False
    if pipe_util.already_step(step_dir, 'picard_merge', logger):
        logger.info('already completed step `merge` of: %s' % outbam_path)
    else:
        logger.info('running step `picard merge of: %s' % outbam_path)
        #tmpfile=os.path.join(merge_dir,'tmpfile')
        home_dir = os.path.expanduser('~')
        cmd = ['java', '-d64', '-jar', os.path.join(home_dir, 'tools/picard-tools/picard.jar'), 'MergeSamFiles', 'USE_THREADING=true', 'ASSUME_SORTED=true', 'SORT_ORDER=coordinate', 'OUTPUT=' + outbam_path, 'TMP_DIR=' + out_dir]
        for input_bam in bam_path_list:
            input_string = 'INPUT=' + input_bam
            cmd.append(input_string)
        if be_lenient:
            cmd.append('VALIDATION_STRINGENCY=LENIENT')
        output = pipe_util.do_command(cmd, logger)

        #save time/mem to db
        df = time_util.store_time(uuid, cmd, output, logger)
        df['bam_path'] = outbam_path
        unique_key_dict = {'uuid': uuid, 'bam_name': outbam_name}
        table_name = 'time_mem_picard_bam_merge'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(step_dir, 'picard_merge', logger)
        logger.info('completed running step `picard merge` of: %s' % outbam_path)
    return outbam_path
