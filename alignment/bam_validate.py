import os
import logging
import sys
from collections import defaultdict
#
import pandas as pd
#
import pipe_util
import df_util
import time_util


def store_validate_error(uuid, bam_path, validate_file, engine, logger):
    val_error_dict = defaultdict(dict)
    with open(validate_file, 'r') as validate_file_open:
        for line in validate_file_open:
            if line.startswith('ERROR:'):
                validation_type = 'ERROR'
                line_split = line.strip().split(',')
                if len(line_split) == 4:
                    line_error = line_split[2:]
                    line_error = ', '.join(line_error)
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                elif len(line_split) == 3:
                    line_error = line_split[2]
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                elif len(line_split) == 2:
                    line_error = line_split[1]
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                elif len(line_split) == 1:
                    line_error = line_split[0]
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                else:
                    logger.debug('validation_type=ERROR')
                    logger.debug('line: %s' % line)
                    logger.debug('Need to handle this comma amount: %s' % len(line_split))
                    sys.exit(1)
            elif line.startswith('WARNING:'):
                validation_type = 'WARNING'
                line_split = line.strip().split(',')
                if len(line_split) == 4:
                    line_error = line_split[2:]
                    line_error = ', '.join(line_error)
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                elif len(line_split) == 3:
                    line_error = line_split[2]
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                elif len(line_split) == 2:
                    line_error = line_split[1].strip()
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                elif len(line_split) == 1:
                    line_error = line_split[0]
                    if line_error in val_error_dict[validation_type].keys():
                        val_error_dict[validation_type][line_error] += 1
                    else:
                        val_error_dict[validation_type][line_error] = 1
                else:
                    logger.debug('validation_type=WARNING')
                    logger.debug('line: %s' % line)
                    logger.debug('Need to handle this comma amount: %s' % len(line_split))
                    sys.exit(1)
            elif line.startswith('No errors found'):
                validation_type = 'PASS'
                line_error = line.strip()
                val_error_dict[validation_type][line_error] = 1
            else:
                logger.info('unknown picard validation line')
                logger.info('line=%s' % line)
                sys.exit(1)


    validation_type = 'ERROR'
    for akey in sorted(val_error_dict[validation_type].keys()):
        store_dict = dict()
        store_dict['value'] = akey
        store_dict['count'] = val_error_dict[validation_type][akey]
        store_dict['uuid'] = [uuid]  # a non scalar
        store_dict['bam_path'] = bam_path
        store_dict['severity'] = validation_type
        logger.info('store_validate_error() store_dict=%s' % store_dict)
        df = pd.DataFrame(store_dict)
        table_name = 'picard_ValidateSamFile'
        unique_key_dict = {'uuid': uuid, 'bam_path': bam_path, 'error': akey}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    validation_type = 'WARNING'
    for akey in sorted(val_error_dict[validation_type].keys()):
        store_dict = dict()
        store_dict['value'] = akey
        store_dict['count'] = val_error_dict[validation_type][akey]
        store_dict['uuid'] = [uuid]  # a non scalar
        store_dict['bam_path'] = bam_path
        store_dict['severity'] = validation_type
        logger.info('store_validate_error() store_dict=%s' % store_dict)
        df = pd.DataFrame(store_dict)
        table_name = 'picard_ValidateSamFile'
        unique_key_dict = {'uuid': uuid, 'bam_path': bam_path, 'error': akey}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    validation_type = 'PASS'
    for akey in sorted(val_error_dict[validation_type].keys()):
        store_dict = dict()
        store_dict['value'] = akey
        store_dict['count'] = val_error_dict[validation_type][akey]
        store_dict['uuid'] = [uuid]  # a non scalar
        store_dict['bam_path'] = bam_path
        store_dict['severity'] = validation_type
        logger.info('store_validate_error() store_dict=%s' % store_dict)
        df = pd.DataFrame(store_dict)
        table_name = 'picard_ValidateSamFile'
        unique_key_dict = {'uuid': uuid, 'bam_path': bam_path, 'error': akey}
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    return

def bam_validate(uuid, bam_path, engine, logger):
    step_dir = os.path.dirname(bam_path)
    validate_file = bam_path + '.validate'
    # Already step left out
    logger.info('running step validate of: %s' % bam_path)
    home_dir = os.path.expanduser('~')
    mo = int((2 ** 32) / 2) - 1

    cmd = ['java', '-d64', '-jar', os.path.join(home_dir, 'tools/picard-tools/picard.jar'), 'ValidateSamFile', 'MO=' + str(mo), 'INPUT=' + bam_path, 'OUTPUT=' + validate_file]
    output = pipe_util.do_command(cmd, logger, allow_fail=True)
    df = time_util.store_time(uuid, cmd, output, logger)
    df['bam_path'] = bam_path
    unique_key_dict = {'uuid': uuid, 'bam_path': bam_path}
    table_name = 'time_mem_picard_ValidateSamFile'
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    logger.info('completed running step validate of: %s' % bam_path)
    # Create already step

    # Already step left out
    logger.info('storing `picard validate` to db')
    store_validate_error(uuid, bam_path, validate_file, engine, logger)
    # Create already step
    logger.info('completed storing `picard validate` to db')
        
                                                    
                        
