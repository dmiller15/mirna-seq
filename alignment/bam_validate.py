import os
import logging
import sys
from collections import defaultdict
#
import pandas as pd
#
import pipe_util


def store_validate_error(uuid, bam_path, validate_file, logger):
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


    validation_type = 'ERROR'
    for akey in sorted(val_error_dict[validation_type].keys()):
        store_dict = dict()
        store_dict['value'] = akey
        store_dict['count'] = val_error_dict[validation_type][akey]
        store_dict['uuid'] = [uuid]  # a non scalar
        store_dict['bam_path'] = bam_path
        store_dict['severity'] = validation_type
        logger.info('store_validate_error() store_dict=%s' % store_dict)
    validation_type = 'WARNING'
    for akey in sorted(val_error_dict[validation_type].keys()):
        store_dict = dict()
        store_dict['value'] = akey
        store_dict['count'] = val_error_dict[validation_type][akey]
        store_dict['uuid'] = [uuid]  # a non scalar
        store_dict['bam_path'] = bam_path
        store_dict['severity'] = validation_type
        logger.info('store_validate_error() store_dict=%s' % store_dict)
    return

def bam_validate(uuid, bam_path, logger):
    step_dir = os.path.dirname(bam_path)
    validate_file = bam_path + '.validate'
    # Already step left out
    logger.info('running step validate of: %s' % bam_path)
    home_dir = os.path.expanduser('~')
    mo = int((2 ** 32) / 2) - 1

    cmd = ['java', '-d64', '-jar', os.path.join(home_dir, 'tools/picard-tools/picard.jar'), 'VallidateSamFile', 'MO=' + str(mo)]
    output = pipe.util.docommand(cmd, logger, allow_fail=True)
    logger.info('completed running step validate of: %s' % bam_path)
        
                                                    
                        
