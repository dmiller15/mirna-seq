import logging
import os
import shlex
import subprocess
import sys
#
import pandas as pd
#
import pipe_util
import fastq_util
import df_util
import time_util

def get_total_deduplicated_percentage(fastqc_data_open, logger):
    for line in fastqc_data_open:
        if line.startswith('#Total Deduplicated Percentage'):
            line_split = line.strip('\n').lstrip('#').split('\t')
            return line_split
    logger.debug('get_total_deduplicated_percentage() failed')
    sys.exit(1)

def fastqc_data_to_dict(data_dict, fastqc_data_path, engine, logger):
    logger.info('fastqc_data_path=%s' % fastqc_data_path)
    values_to_store = ['Encoding', 'Total Sequences', 'Sequences flagged as poor quality', 'Sequence length', '%GC']
    with open(fastqc_data_path, 'r') as fastqc_data_open:
        for line in fastqc_data_open:
            for value_to_store in values_to_store:
                if line.startswith(value_to_store):
                    if value_to_store == '%GC':  # #or mysql fails on table update
                        value_to_store = 'percentGC'
                    data_value = line.split('\t')[1].strip()
                    data_dict[value_to_store] = data_value
    return data_dict

def fastqc_detail_to_df(uuid, fq_path, fastqc_data_path, data_key, engine, logger):
    logger.info('detail step: %s'  % data_key)
    logger.info('fastqc_data_path: %s' % fastqc_data_path)
    process_data = False
    process_header = False
    have_data = False
    with open(fastqc_data_path, 'r') as fastqc_data_open:
        for line in fastqc_data_open:
            #logger.info('line=%s' % line)
            if line.startswith('##FastQC'):
                #logger.info('\tcase 1')
                continue
            elif process_data and not process_header and line.startswith('>>END_MODULE'):
                #logger.info('\tcase 2')
                break
            elif line.startswith(data_key):
                #logger.info('\tcase 3')
                logger.info('fastqc_detail_to_df() found data_key: %s' % data_key)
                process_data = True
            elif process_data and line.startswith('>>END_MODULE'):
                #logger.info('\tcase 4')
                logger.info('fastqc_detail_to_df() >>END_MODULE')
                if data_key == '>>Basic Statistics':
                    value_list = get_total_deduplicated_percentage(fastqc_data_open, logger)
                    row_df = pd.DataFrame([uuid, fq_path] + value_list)
                    row_df_t = row_df.T
                    row_df_t.columns = ['uuid', 'fastq_path'] + header_list
                    #logger.info('9 row_df_t=%s' % row_df_t)
                    df = df.append(row_df_t)
                break
            elif process_data and line.startswith('#'):
                #logger.info('\tcase 5')
                process_header = True
                header_list = line.strip('#').strip().split('\t')
                logger.info('fastqc_detail_to_df() header_list: %s' % header_list)
            elif process_data and process_header:
                #logger.info('\tcase 6')
                logger.info('fastqc_detail_to_df() columns=%s' % header_list)
                df = pd.DataFrame(columns = ['uuid', 'fastq_path'] + header_list)
                process_header = False
                have_data = True
                #logger.info('2 df=%s' % df)
                line_split = line.strip('\n').split('\t')
                logger.info('process_header line_split=%s' % line_split)
                row_df = pd.DataFrame([uuid, fq_path] + line_split)
                row_df_t = row_df.T
                row_df_t.columns = ['uuid', 'fastq_path'] + header_list
                logger.info('1 row_df_t=%s' % row_df_t)
                df = df.append(row_df_t)
                #logger.info('3 df=%s' % df)
            elif process_data and not process_header:
                #logger.info('\tcase 7')
                line_split = line.strip('\n').split('\t')
                logger.info('not process_header line_split=%s' % line_split)
                row_df = pd.DataFrame([uuid, fq_path] + line_split)
                row_df_t = row_df.T
                row_df_t.columns = ['uuid', 'fastq_path'] + header_list
                logger.info('not process_header line_split=%s' % line_split)
                logger.info('2 row_df_t=%s' % row_df_t)
                df = df.append(row_df_t)
                #logger.info('4 df=%s' % df)
            elif not process_data and not process_header:
                #logger.info('\tcase 8')
                continue
            else:
                #logger.info('\tcase 9')
                logger.debug('fastqc_detail_to_df(): should not be here')
                sys.exit(1)
    if have_data:
        logger.info('complete df=%s' % df)
        return df
    else:
        logger.info('no df')
        return None
    logger.debug('fastqc_detail_to_df(): should not reach end of function')
    sys.exit(1)

def fastqc_summary_to_dict(data_dict, fastqc_summary_path, engine, logger):
    logger.info('fastqc_summary_path=%s' % fastqc_summary_path)
    with open(fastqc_summary_path, 'r') as fastqc_summary_open:
        for line in fastqc_summary_open:
            line_split = line.split('\t')
            line_key = line_split[1].strip()
            line_value = line_split[0].strip()
            data_dict[line_key] = line_value
    return data_dict

def fastqc_to_db(uuid, fq_path, engine, logger):
    fastq_name = os.path.basename(fq_path)
    fastq_dir = os.path.dirname(fq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    fastq_base = fastq_base.rstrip('.fq')
    qc_report_dir = os.path.join(fastq_dir, fastq_base + '_fastqc')
    fastqc_data_path = os.path.join(qc_report_dir, 'fastqc_data.txt')
    fastqc_summary_path = os.path.join(qc_report_dir, 'summary.txt')
    if pipe_util.already_step(fastq_dir, 'fastqc_db_' + fastq_base, logger):
        logger.info('already completed step `fastqc db`: %s' % fq_path)
    else:
        logger.info('writing `fastqc db`: %s' % fq_path)
        summary_dict = dict()
        summary_dict['uuid'] = [uuid]
        summary_dict['fastq_name'] = fastq_name
        summary_dict = fastqc_summary_to_dict(summary_dict, fastqc_summary_path, engine, logger)        
        df = pd.DataFrame(summary_dict)
        table_name = 'fastq_summary'
        unique_key_dict = {'uuid': uuid, 'fastq_name': fastq_name}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        
        data_key_list = ['>>Basic Statistics', '>>Per base sequence quality', '>>Per tile sequence quality',
                         '>>Per sequence quality scores', '>>Per base sequence content', '>>Per sequence GC content',
                         '>>Per base N content', '>>Sequence Length Distribution', '>>Sequence Duplication Levels',
                         '>>Overrepresented sequences', '>>Adapter Content', '>>Kmer Content']
        for data_key in data_key_list:
            df = fastqc_detail_to_df(uuid, fq_path, fastqc_data_path, data_key, engine, logger)
            if df is None:
                continue
            table_name = 'fastqc_data_' + '_'.join(data_key.lstrip('>>').strip().split(' '))
            logger.info('fastqc_to_db() table_name=%s' % table_name)
            unique_key_dict = {'uuid': uuid, 'fastq_path': fq_path}
            df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
            
        pipe_util.create_already_step(fastq_dir, 'fastqc_db_' + fastq_base, logger)
        logger.info('completed writing `fastqc db`: %s' % fq_path)
    return

def guess_enc_db(uuid, fq_path, engine, logger):
    fastq_dir = os.path.dirname(fq_path)
    fastq_name = os.path.basename(fq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    guess_enc_path = fq_path + '.format'
    guess_enc_value = str()
    with open(guess_enc_path, 'r') as guess_enc_open:
        guess_enc_value = guess_enc_open.readline().strip()
    data_dict = dict()
    if pipe_util.already_step(fastq_dir, 'fastq_encdb_' + fastq_base, logger):
        logger.info('writing `guess_enc_db`: %s' %fq_path)
    else:
        logger.info('writing `guess_enc_db`: %s' % fq_path)
        data_dict['uuid'] = [uuid]
        data_dict['fastq_name'] = fastq_name
        data_dict['guess'] = guess_enc_value
        df = pd.DataFrame(data_dict)
        table_name = 'guess_fastq_encoding'
        unique_key_dict = {'uuid': uuid, 'fastq_name': fastq_name}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(fastq_dir, 'fastq_encdb_' + fastq_base, logger)
        logger.info('completed writing `guess_enc_db`: %s' % fq_path)
    return

def do_fastqc(uuid, fastq_path, engine, logger):
    fastq_name = os.path.basename(fastq_path)
    fastq_dir = os.path.dirname(fastq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    if pipe_util.already_step(fastq_dir, 'fastqc_' + fastq_base, logger):
        logger.info('already completed step `fastqc`: %s' % fastq_path)
    else:
        logger.info('running step `fastqc`: %s' % fastq_path)
        cmd = ['/home/ubuntu/tools/FastQC/fastqc', '--extract', fastq_path] # fix the path here
        output = pipe_util.do_command(cmd, logger)
        df = time_util.store_time(uuid, cmd, output, logger)
        df['fastq_path'] = fastq_path
        table_name = 'time_mem_fastqc'
        unique_key_dict = {'uuid': uuid, 'fastq_path': fastq_path}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        pipe_util.create_already_step(fastq_dir, 'fastqc_' + fastq_base, logger)
    return

def get_fastq_size(fq_path, logger):
    fq_dir = os.path.dirname(fq_path)
    fq_name = os.path.basename(fq_path)
    size_file = os.path.join(fq_dir, fq_name + '.size')
    if pipe_util.already_step(fq_dir, fq_name + 'size', logger):
        with open(size_file, 'r') as size_open:
            size_str = size_open.readline()
        size_value = int(size_str)
        logger.info('already determined size of fq %s: %s' % (fq_name, str(size_value)))
        return size_value
    else:
        logger.info('determining size of fq %s' % fq_name)
        size_value = os.path.getsize(fq_path)
        with open(size_file, 'w') as size_open:
            size_open.write(str(size_value))
        pipe_util.create_already_step(fq_dir, fq_name + 'size', logger)
        logger.info('determined size of fq %s: %s' % (fq_name, str(size_value)))
        return size_value

def fastq_guess_encoding(uuid, fastq_dir, engine, logger): # future thread count
    logger.info('enter fastq_guess_encoding()')
    fastqlist = fastq_util.buildfastqlist(fastq_dir, logger)
    logging.info('fastqlist=%s' % fastqlist)
    sefastqlist = fastq_util.buildsefastqlist(fastqlist)
    logger.info('sefastqlist=%s' % sefastqlist)
    for seread in sefastqlist:
        fq_path = os.path.join(fastq_dir, seread)
        fq_path_size = get_fastq_size(fq_path, logger)
        if fq_path_size > 0:
            do_guess_encoding(uuid, fq_path, engine, logger)
            guess_enc_db(uuid, fq_path, engine, logger)
            

def fastq_validate(uuid, fastq_dir, engine, logger):
    fastqlist = fastq_util.buildfastqlist(fastq_dir, logger)
    logger.info('fastqlist=%s' % fastqlist)
    sefastqlist = fastq_util.buildsefastqlist(fastqlist)
    for seread in sefastqlist:
        fq_path = os.path.join(fastq_dir, seread)
        fq_path_size = get_fastq_size(fq_path, logger)
        if fq_path_size > 0:
            do_fastqc(uuid, fq_path, engine, logger)
            fastqc_to_db(uuid, fq_path, engine, logger)
    return

def write_fastq_format(fastq_path, output, logger):
    outfile = fastq_path + '.format'
    # need to read last txt line instead (2nd to last line)
    output_list = output.decode().split('\n')
    logger.info('fastq guess list: %s' % output_list)
    outstring = output_list[-2].strip()
    if outstring == 'Exit status: 0': # handle awk terminating prematurely when guess happens before EOF
        outstring = output_list[1].strip().split('\t')[0]
    outfile_open = open(outfile, 'w')
    outfile_open.write(outstring)
    outfile_open.close()
    if outstring == 'Illumina-1.8':
        logger.info('write_fastq_format() guessed encoding: %s' % outstring)
        return

def do_guess_encoding(uuid, fastq_path, engine, logger):
    fastq_name = os.path.basename(fastq_path)
    fastq_dir = os.path.dirname(fastq_path)
    fastq_base, fastq_ext = os.path.splitext(fastq_name)
    if pipe_util.already_step(fastq_dir, 'guess_' + fastq_base, logger):
        logger.info('already completed step `guess_encoding`: %s' % fastq_path)
    else:
        logger.info('running step `guess encoding` of %s' % fastq_path)
        pipe_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
        guess_path = os.path.join(pipe_dir, 'guess-encoding.py')
        guess_cmd = 'python2 ' + guess_path
        time_cmd = '/usr/bin/time -v ' + guess_cmd + ' -f ' + fastq_path
        proc = subprocess.Popen(time_cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        output = proc.communicate()[0]
        logger.info('output=%s' % output)
        df = time_util.store_time(uuid, time_cmd, output, logger)
        df['fastq_path'] = fastq_path
        table_name = 'time_mem_guessencoding'
        unique_key_dict = {'uuid': uuid, 'fastq_path': fastq_path}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        logger.info('do_guess_encoding output=%s' % output.decode())
        write_fastq_format(fastq_path, output, logger)
        pipe_util.create_already_step(fastq_dir, 'guess_' + fastq_base, logger)
    return
