import glob
import os
import logging
import sys
#
import fastq_util
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
    if pipe_util.already_step(fastq_dir, 'fastq', logger):
        logger.info('already completed step `bamtofastq` of: %s' % bam_path)
    else:
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
        pipe_util.create_already_step(fastq_dir, 'fastq', logger)
        logger.info('completed running step `bamtofastq` of: %s' % bam_path)
    return

def get_readgroup_str(readname, readgroup_path_dict, logger):
    fq_split = readname.split('_')
    read_ext = fq_split[-1]
    fq_split.remove(read_ext)
    rg_name = '_'.join(fq_split)
    logger.info('get_readgroupstr() rg_name=%s' % rg_name)
    if rg_name == 'default':
        logger.info('%s has no readgroup info, so returning None' % rg_name)
        return None
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
    step_dir = os.path.dirname(bam_path)
    if pipe_util.already_step(step_dir, 'readgroups', logger):
        logger.info('already extracted readgroups from %s' % bam_path)
        readgroup_path_list = glob.glob(os.path.join(step_dir, '*.RG'))
        readgroup_path_dict = dict()
        for readgroup_path in readgroup_path_list:
            readgroup_file = os.path.basename(readgroup_path)
            readgroup = readgroup_file.rstrip('.RG')
            readgroup_path_dict[readgroup] = readgroup_path
        return readgroup_path_dict
    else:
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
            if pipe_util.already_step(bam_dir, readgroup['ID'] + '_rg_file', logger):
                logger.info('already wrote @RG to: %s' % outfile_path)
            else:
                outfile_open = open(outfile_path, 'w')
                outstring = '@RG'
                for rg_key in sorted(readgroup.keys()):
                    outstring += '\\t' + rg_key + ':' + readgroup[rg_key]
                outfile_open.write(outstring)
                outfile_open.close()
                pipe_util.create_already_step(bam_dir, readgroup['ID'] + '_rg_file', logger)
        logger.info('readgroup_path_dict=%s' % readgroup_path_dict)
        pipe_util.create_already_step(step_dir, 'readgroups', logger)
        logger.info('completed extracting readgroups from %s' % bam_path)

    # Store @RG to db
    if pipe_util.already_step(step_dir, 'readgroups_db', logger):
        logger.info('already stored readgroups of %s to db' % bam_path)
    else:
        logger.info('storing readgroups of %s to db' % bam_path)
        for readgroup in readgroups:
            if pipe_util.already_step(bam_dir, readgroup['ID'] + '_rg_db', logger):
                logger.info('already wrote %s to db' % readgroup['ID'])
            else:
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
                    pipe_util.create_already_step(bam_dir, readgroup['ID'] + '_rg_db', logger)
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
    if pipe_util.already_step(se_realn_dir, readkey + '_sai_' + fastqbasename, logger):
        logger.info('already completed step `bwa aln` of: %s' % read1)
    else:
        aln_frontend = ['bwa', 'aln', reference_fasta_path, f1]
        
        if fastq_encoding == 'Illumina-1.8' or fastq_encoding == 'Sanger / Illumina 1.9':
            logger.info('%s is fastq_encoding, so use `bwa aln`' % fastq_encoding)
        elif fastq_encoding == 'Illumina-1.3' or fastq_encoding == 'Illumina-1.5' or fastq_encoding == 'Illumina-1.5-HMS':
            logger.info('%s is fastq_encoding, so use `bwa aln -I`' % fastq_encoding)
            aln_frontend.insert(3, '-I')
        else:
            logger.info('unhandled fastq_encoding: %s' % fastq_encoding)
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
        pipe_util.create_already_step(se_realn_dir, readkey + '_sai_' + fastqbasename, logger)

    # BWA SAMSE Command
    if pipe_util.already_step(se_realn_dir, readkey + '_samse_' + fastqbasename, logger):
        logger.info('already completed set `bwa samse` of %s:' % outbam_path)
    else:
        if rg_str is None:
            samse_cmd = ['bwa', 'samse', '-n 10', reference_fasta_path, outsai_path, f1]
        else:
            samse_cmd = ['bwa', 'samse', '-n 10', reference_fasta_path, '-r' + '"' + rg_str + '"', outsai_path, f1]
        samtools_cmd = 'samtools view -Shb -o ' + outbam_path + ' -'
        shell_samse_cmd = ' '.join(samse_cmd)
        shell_cmd = shell_samse_cmd + ' | ' + samtools_cmd
        logger.info('bwa_aln_single() shell_cmd=%s' % shell_cmd)
        samse_output = pipe_util.do_shell_command(shell_cmd, logger)
        df = time_util.store_time(uuid, shell_cmd, samse_output, logger)
        logger.info('bwa_aln_single() df=%s' % df)
        df['bam_path'] = bam_path
        df['reference_fasta_path'] = reference_fasta_path
        unique_key_dict = {'uuid': uuid, 'bam_path': outbam_path, 'reference_fasta_path': reference_fasta_path}
        table_name = 'time_mem_bwa_samse'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        logger.info('completed running step `bwa single samse` of: %s' % bam_path)
        pipe_util.create_already_step(se_realn_dir, readkey + '_samse_' + fastqbasename, logger)
    return outbam_path

def get_fastq_encoding_from_db(fastq_name, fastq_dir, engine, logger):
    fastq_path = os.path.join(fastq_dir, fastq_name)
    df = pd.read_sql_query('select * from fastqc_data_Basic_Statistics where Measure="Encoding" and fastq_path="'+fastq_path+'"', engine)
    if len(df) != 1:
        logger.debug('There should only be one fastq: %s' % fastq_name)
        logger.debug('df = %s' % df)
        sys.exit(1)
    else:
        illumina_encoding = df['Value'][0]
    return illumina_encoding

def get_fastq_length_from_db(fastq_name, fastq_dir, engine, logger):
    fastq_path = os.path.join(fastq_dir, fastq_name)
    df = pd.read_sql_query('select * from fastqc_data_Basic_Statistics where Measure="Sequence length" and fastq_path="'+fastq_path+'"', engine)
    if len(df) != 1:
        logger.debug('There should only be one fastq: %s' % fastq_name)
        logger.debug('df = %s' % df)
        sys.exit(1)
    else:
        fastq_length = 15 # Placeholder for int(df['Value'][0])
    return fastq_length

def get_max_fastq_length_from_db(engine, logger):
    df = pd.read_sql_query('select * from fastqc_data_Basic_Statistics where Measure="Sequence length"', engine)
    max_fastq_length = int(max(list(df['Value'])))
    return max_fastq_length

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
        seread_fastq_encoding = get_fastq_encoding_from_db(seread, fastq_dir, engine, logger)
        seread_fastq_length = get_fastq_length_from_db(seread, fastq_dir, engine, logger)
        rg_str = get_readgroup_str(seread, readgroup_path_dict, logger) 
        bam_path = bwa_aln_single(uuid, bam_path, fastq_dir, seread, realn_dir, 's', reference_fasta_path, rg_str, seread_fastq_encoding, engine, logger)
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
