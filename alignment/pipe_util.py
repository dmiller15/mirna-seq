import os
import sys
import subprocess 
import logging
import time
import shutil

def update_env(logger):
    env = dict()
    env.update(os.environ)
    path = env['PATH']
    logger.info('path=%s' % path)
    home_dir = os.path.expanduser('~')
    new_path = path
    new_path += ':' + os.path.join(home_dir, 'tools', 'biobambam', 'bin')
    new_path += ':' + os.path.join(home_dir, 'tools', 'samtools')
    new_path += ':' + os.path.join(home_dir, 'tools', 'bwa.kit')
    new_path += ':' + os.path.join(home_dir, 'tools')
    pipe_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    new_path += ':' + pipe_dir
    logger.info('new_path=%s' % new_path)
    env['PATH'] = new_path
    return env

def do_command(cmd, logger, stdout=subprocess.STDOUT, stderr=subprocess.PIPE, allow_fail=False):
    env = update_env(logger)
    timecmd = cmd
    timecmd.insert(0, '/usr/bin/time')
    timecmd.insert(1, '-v')
    logger.info('running cmd: %s' % timecmd)
    try:
        output = subprocess.check_output(timecmd, env=env, stderr=subprocess.STDOUT)
        logger.info('contents of output(s)=%s' % output.decode().format())
    except Exception as e:
        logger.debug('failed cmd: %s' % str(timecmd))
        logger.debug('exception: %s' % e)
        if allow_fail:
            return e.output
        else:
            sys.exit('failed cmd: %s' % str(timecmd))
    logger.info('completed cmd: %s' % str(timecmd))
    return output

def do_shell_command(cmd, logger, stdout=subprocess.STDOUT, stderr=subprocess.PIPE):
    env = update_env(logger)
    timecmd = '/usr/bin/time -v ' + cmd
    logger.info('running cmd: %s' % timecmd)
    try:
        output = subprocess.check_output(timecmd, env=env, stderr=subprocess.STDOUT, shell=True)
        logger.info('contents of output(s)=%s' % output.decode().format())
    except Exception as e:
        logger.debug('failed cmd: %s' % str(timecmd))
        logger.debug(e.output)
        logger.debug('exception: %s' % e)
        sys.exit('failed cmd: %s' % str(timecmd))
    logger.info('completed cmd: %s' % str(timecmd))
    return output

def touch(fname, logger, mode=0o666, dir_fd=None, **kwargs):
    logger.info('creating empty file: %s' % fname)
    flags = os.O_CREAT | os.O_APPEND
    with os.fdopen(os.open(fname, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(f.fileno() if os.utime in os.supports_fd else fname,
                 dir_fd=None if os.supports_fd else dir_fd, **kwargs)

def already_step(step_dir, step, logger):
    have_step_flag = os.path.join(step_dir, 'have_' + step)
    if os.path.exists(have_step_flag):
        logger.info('step flag exists: %s' % have_step_flag)
        return True
    else:
        logger.info('step flag does not exist:%s' % have_step_flag)
        return False

def create_already_step(step_dir, step, logger):
    have_step_flag = os.path.join(step_dir, 'have_' + step)
    touch(have_step_flag, logger)

def is_aln_bam(bam_path, logger):
    bwa_type = os.path.basename(os.path.dirname(bam_path))
    bam_dirs = bam_path.split('/')
    for bam_dir in bam_dirs:
        if bam_dir.startswith('bwa'):
            if bam_dir.startswith('bwa_aln'):
                return True
            else:
                return False
    logger.debug('no `bwa` in path')
    sys.exit(1)
