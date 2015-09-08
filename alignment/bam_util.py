import glob
import os
import logging
import sys
#
import pipe_util
import df_util
import time_util

def bam_to_fastq(uuid, bam_path, engine, logger):
    step_dir = os.path.dirname(bam_path)
    uuid_dir = step_dir

    logger.info('uuid_dir is: %s' % uuid_dir)
    fastq_dir = os.path.join(uuid_dir, 'fastq')
    logger.info('fastq_dir is: %s' % fastq_dir)
    # Omit already step for now
    logger.info('running step `bamtofastq` of %s: ' % bam_path)
    os.makedirs(fastq_dir, exist_ok=True)
    tempfq = os.path.join(fastq_dir, 'tempfq')
    cmd = ['bamtofastq', 'S=%s' % uuid + '.fq', 'filename=' + bam_path, 'outputdir=' + fastq_dir, 'tryoq=1', 'collate=1', 'outputperreadgroup=1', 'T=' + tempfq]
    output = pipe_util.do_command(cmd, logger)
    df = time_util.store_time(uuid, cmd, output, logger)
    df['bam_path'] = bam_path
    unique_key_dict = {'uuid': uuid, 'bam_path': bam_path}
    table_name = 'time_mem_bamtofastq'
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    # Create already step
    logger.info('completed running step `bamtofastq` of: %s' % bam_path)

def bwa_aln_single(uuid, bam_path, fastq_dir, read1, realn_dir, reference_fasta_path, engine, logger):
    se_realn_dir = os.path.join(realn_dir, 'bwa_aln')
    logger.info('se_realln_dir=%s' % se_realn_dir)
    logger.info('read1=%s' % read1)
    fastqbasename = read1.replace('.fq', '')
    logger.info('fastqbasename=%s' % fastqbasename)
    outsai = os.path.basename(fastqbasename + '.sai')
    outbam = os.path.basename(fastqbasename + '.bam')
    outsai_path = os.path.join(se_realn_dir, outsai)
    outbam_path = os.path.join(se_realn_dir, outbam)
    read1_name, read1_ext = os.path.splitext(read1)
    sai1_name = read1_name + '.sai'
    sai1_path = os.path.join(se_realn_dir, sai1_name)
    f1 = os.path.join(fastq_dir, read1)
    os.makedirs(se_realn_dir, exist_ok=True)

    # Already step check
    # BWA ALN Command
    aln_cmd = ['bwa', 'aln', reference_fasta_path, f1, ' > ', outsai_path]
    shell_aln_cmd = ' '.join(aln_cmd)
    aln_output = pipe_util.do_shell_command(shell_aln_cmd, logger)
    df = time_util.store_time(uuid, shell_aln_cmd, logger)
    df['sai_path'] = outsai_path
    df['reference_fasta_path'] = reference_fasta_path
    # df['thread_count'] = thread_count
    unique_key_dict = {'uuid': uuid, 'sai_path': outsai_path, 'reference_fasta_path': reference_fasta_path} # 'thread_count': thread_count}
    table_name = 'time_mem_bwa_aln'
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    logger.info('completed running step `bwa single aln` of: %s' % bam_path)
    # Create already step

    # Already step check
    # BWA SAMSE Command
    samse_cmd = ['bwa', 'samse', '-n 10', reference_fasta_path, outsai_path, f1]
    samtools_cmd = 'samtools view -Shb -o ' + outbam_path + ' -'
    shell_samse_cmd = ' '.join(samse_cmd)
    shell_cmd = shell_samse_cmd + ' | ' + samtools_cmd
    samse_output = pipe_util.do_shell_command(shell_cmd, logger)
    df = time_util.store_time(uuid, shell_cmd, samse_output, logger)
    df['bam_path'] = bam_path
    df['reference_fasta_path'] = reference_fasta_path
    unique_key_dict = {'uuid': uuid, 'bam_path': outbam_path, 'reference_fasta_path': reference_fasta_path}
    table_name = 'time_mem_bwa_samse'
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    logger.info('completed running step `bwa single samse` of: %s' % bam_path)
    # Create already step
    return outbam_path

def main():
    print ('hello')

if __name__ == '__main__':
    main()
