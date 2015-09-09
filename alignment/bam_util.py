import glob
import os
import logging
import sys
#
import pipe_util
import df_util
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

def get_readgroup_str(readname, readgroup_path_dict, logger):
    fq_split = readname.split('_')
    read_ext = fq_split[-1]
    fq_split.remove(read_ext)
    rg_name = '_'.join(fq_split)
    readgroup_path = readgroup_path_dict[rg_name]
    with open(readgroup_path, 'r') as readgroup_path_open:
        readgroup_line = readgroup_path_open.readline()
    logger.info('readgroup_line=%s' % readgroup_line)
    return readgroup_line

def header_rg_list_to_rg_dicts(header_rg_list):
    readgroups_list = list()
    for rg_list in header_rg_list:
        keys_values = rg_list.lstrip('@RG').lstrip('\t').lstrip('@RG').lstrip('\t').split('\t')
        readgroup = dict()
        for key_value in keys_values:
            key_value_split = key_value.split(':')
            a_key = key_value_split[0]
            a_value = key_value_split[1]
            readgroup[a_key] = a_value
        readgroups_list.append(readgroup)
    return readgroups_list

def write_readgroups(uuid, bam_path, engine, logger):
    # step_dir = os.path.dirname(bam_path)
    # Already step omitted
    logger.info('extracting readgroups from %s' % bam_path)
    bam_dir = os.path.dirname(bam_path)
    samfile = pysam.AlignmentFile(bam_path, 'rb')
    header = samfile.text
    header_list = header.split('\n')
    header_rg_list = [ header_line for header_line in header_list if header_line.startswith('@RG') ]
    readgroups = header_rg_list_to_rg_dicts(header_rg_list)

    readgroup_path_dict = dict()
    for readgroup in readgroups:
        rg_id = readgroup['ID']
        outfile = rg_id + '.RG'
        outfile_path = os.path.join(bam_dir, outfile)
        readgroup_path_dict[rg_id] = outfile_path
        # alread step omitted
        outfile_open = open(outfile_path, 'w')
        outstring = '@RG'
        for rg_key in sorted(readgroup.keys()):
            outstring += '\\t' + rg_key + ':' + readgroup[rg_key]
        outfile_open.write(outstring)
        outfile_open.close()
    logger.info('readgroup_path_dict=%s' % readgroup_path_dict)
    # create already step omitted
    logger.info('completed extracting readgroups from %s' % bam_path)

    # Store @RG to db
    # omitted already step
    logger.info('storing readgroups of %s to db' % bam_path)
    for readgroup in readgroups:
        # omitted already step
        readgroup['uuid'] = [uuid]
        table_name = 'readgroups'
        for rg_key in sorted(readgroup.keys()):
            rg_dict = dict()
            rg_dict['uuid'] = [uuid]
            rg_dict['ID'] = readgroup['ID']
            rg_dict['value'] = readgroup[rg_key]
            df = pd.DataFrame(rg_dict)
            unique_key_dict = {'uuid': uuid, 'ID': readgroup['ID'], 'key': rg_key}
            df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
            # create already step omitted
        logger.info('completed storing @RG %s to db' % readgroup['ID'])
    return readgroup_path_dict

def bwa_aln_single(uuid, bam_path, fastq_dir, read1, realn_dir, readkey, reference_fasta_path, rg_str, fastq_encoding, engine, logger):
    se_realn_dir = os.path.join(realn_dir, 'bwa_aln_' + readkey)
    logger.info('se_realln_dir=%s' % se_realn_dir)
    logger.info('read1=%s' % read1)
    fastqbasename = read1.replace('_' + readkey + '.fq', '')
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

    # BWA ALN Command
    # Already step check omitted
    aln_frontend = ['bwa', 'aln', reference_fasta_path, f1]
    if fastq_encoding == 'Illumina_1.8':
        logger.info('%s is fastq_encoding, so use `bwa aln`' % fastq_encoding)
    elif fastq_encoding == 'Illumina-1.3' or fastq_encoding == 'Illumina-1.5' or fastq_encoding == 'Illumina-1.5-HMS':
        logger.info('%s is fastq_encoding, so use `bwa aln -I`' % fastq_encoding)
        aln_frontend.insert(3, '-I')
    else:
        logger.info('unhandled fastq_encoding: ' % fastq_encoding)
        sys.exit(1)
    aln_backend = [ ' > ', outsai_path ]
    aln_cmd = aln_frontend + aln_backend
    shell_aln_cmd = ' '.join(aln_cmd)
    aln_output = pipe_util.do_shell_command(shell_aln_cmd, logger)
    df = time_util.store_time(uuid, shell_aln_cmd, aln_output, logger)
    df['sai_path'] = outsai_path
    df['reference_fasta_path'] = reference_fasta_path
    # df['thread_count'] = thread_count
    unique_key_dict = {'uuid': uuid, 'sai_path': outsai_path, 'reference_fasta_path': reference_fasta_path} # 'thread_count': thread_count}
    table_name = 'time_mem_bwa_aln'
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    logger.info('completed running step `bwa single aln` of: %s' % bam_path)
    # Create already step omitted

    # BWA SAMSE Command
    # Already step check omitted
    samse_cmd = ['bwa', 'samse', '-n 10', reference_fasta_path, '-r' + '"' + rg_str + '"', outsai_path, f1]
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
    # Create already step omitted
    return outbam_path

def bwa(uuid, bam_path, reference_fasta_path, readgroup_path_dict, engine, logger):
    uuid_dir = os.path.dirname(bam_path)
    logger.info('uuid_dir=%s' % uuid_dir)
    fastq_dir = os.path.join(uuid_dir, 'fastq')
    logger.info('fastq_dir=%s' % fastq_dir)
    realn_dir = os.path.join(uuid_dir, 'realn')
    os.makedirs(realn_dir, exist_ok=True)
    fastqlist = fastq_util.buildfastqlist(fastq_dir, logger)
    logger.info('fastqlist=%s' % fastqlist)
    sefastqlist = fastq_util.buildsefastqlist(fastqlist)
    logger.info('sefastqlist=%s' % sefastqlist)
    bam_path_list = list()
    for seread in sefastqlist:
        seread_fastq_encoding = get_fastq_encoding(seread, fastq_dir, logger)
        rg_str = get_readgroup_str(uuid, fastq_dir, seread, engine, logger) #soon to be in bam_util
        bam_path = bwa_aln_single(uuid, bam_path, fastq_dir, seread, realn_dir, 's', reference_fasta_path, rg_str, seread_fastq_encoding, enging, logger)
        bam_path_list.append(bam_path)
    return bam_path_list

def get_fastq_encoding(fastq_name, fastq_dir, logger):
    fastq_format_path = os.path.join(fastq_dir, fastq_name + '.format')
    with open(fastq_format_path, 'r') as fastq_format_open:
        fastq_format = fastq_format_open.readline()
    return fastq_format.strip()

    
def main():
    print ('hello')

if __name__ == '__main__':
    main()
