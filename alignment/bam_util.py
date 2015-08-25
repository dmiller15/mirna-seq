import glob
import os
import logging
import sys
#
import pipe_util

def new_bam_to_fastq(uuid, bam_path, logger):
    step_dir = os.path.dirname(bam_path)
    uuid_dir = step_dir

    logger.info('uuid_dir is: %s' % uuid_dir)
    fastq_dir = os.path.join(uuid_dir, 'fastq')
    logger.info('running step `bamtofastq` of %s: ' % bam_path)
    os.makedirs(fastq_dir, exist_ok=True)
    tempfq = os.path.join(fastq_dir, 'tempfq')
    cmd = ['bamtofastq', 'S=%s' % uuid + '.fq', 'filename=' + bam_path, 'outputdir=' + fastq_dir, 'tryoq=1', 'collate=1', 'outputperreadgroup=1', 'T=' + tempfq]
    output = pipe_util.do_command(cmd, logger) 
    logger.info('completed running step `bamtofastq` of: %s' % bam_path)

def new_bwa_aln_single(uuid, bam_path, fastq_dir, read1, realn_dir, reference_fasta_path,logger):
    se_realn_dir = os.path.join(realn_dir, 'bwa_aln')
    logger.info('se_realln_dir=%s' % se_realn_dir)
    logger.info('read1=%s' % read1)
    fastqbasename = read1.replace('.fq', '')
    logger.info('fastqbasename=%s' % fastqbasename)
    outsai = os.path.basename(fastqbasename + '.sai')
    outbam = os.path.basename(fastqbasename + '.bam')
    outsai_path = os.path.join(se_realn_dir, outsai)
    outbam_path = os.path.join(se_realn_dir, outbam)
    f1 = os.path.join(fastq_dir, read1)
    os.makedirs(se_realn_dir, exist_ok=True)
    # BWA ALN Command
    aln_cmd = ['bwa', 'aln', reference_fasta_path, f1, ' > ', outsai_path]
    shell_aln_cmd = ' '.join(aln_cmd)
    aln_output = pipe_util.do_shell_command(shell_aln_cmd, logger)
    logger.info('completed running step `bwa single aln` of: %s' % bam_path)
    # BWA SAMSE Command
    samse_cmd = ['bwa', 'samse', '-n 10', reference_fasta_path, outsai_path, f1]
    samtools_cmd = 'samtools view -Shb -o ' + outbam_path + ' -'
    shell_samse_cmd = ' '.join(samse_cmd)
    shell_cmd = shell_samse_cmd + ' | ' + samtools_cmd
    samse_output = pipe_util.do_shell_command(shell_cmd, logger)
    logger.info('completed running step `bwa single samse` of: %s' % bam_path)
    return outbam_path

def main():
    print ('hello')

if __name__ == '__main__':
    main()
