# Parallel rsync Transfer Program
# Transfers files/directories from remote rsync servers load-balanced
#     round-robin in parallel to a local filesystem using rsync
# Brandon White, 2022

import argparse
import getpass
import logging
import math
import os
import os.path
import subprocess
import tempfile
from multiprocessing import Process, Queue

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger()

ONE_TERABYTE = math.pow(1024, 4)
TARBALL_SIZE_LIMIT = ONE_TERABYTE

class Sentinel:
    pass

#def execute_transfer(pid, local_directory, remote_host, transfer_path, user, pwd_f, fail_logger):
#    module = 'LSSTUser' # TODO: Make this an actual argument
#    remote_source = f'{user}@{remote_host}::{module}/{transfer_path}'
#    format_string = '--out-format=\"%o %m %i %n %l %C\"' # TODO: Make this an actual argument
#    pwd_arg = f'--password-file={pwd_f}'
#    logger.info(f'(pid:{pid}) Executing transfer of {transfer_path} from {remote_host} to {local_directory}\n\t\
#            rsync --archive --relative --xattrs {pwd_arg} {format_string} {remote_source} {local_directory}')
#    xfer_process = subprocess.Popen([
#        'rsync',
#        '--archive',
#        '--relative',
#        '--xattrs',
#        pwd_arg,
#        format_string,
#        remote_source,
#        local_directory
#    ], stdout=subprocess.PIPE)
#    xfer_stdout, xfer_stderr = xfer_process.communicate()
#    logger.info(f'(pid:{pid}) {xfer_stdout.decode().strip()}')
#    if xfer_process.returncode != 0:
#        logger.info(f'!!!  TRANSFER FAILURE: {remote_source}')
#        fail_logger.error(transfer_path)
def execute_tar(pid, tmp_tarfile_input_list):
	pass

def do_processing(pid, tar_queue, args):
    tar_rolling_size = 0 # Hold the accumulation of the list of files to be tarred in this batch
    while True:
        tar_info = tar_queue.get()
	tar_info = tar_info.split()
	file_size = tar_info[0]
	file_path = tar_info[1]

        if isinstance(tar_info, Sentinel):
            logger.info(f'PID: {pid} complete. Waiting to join.')
            return

	
	if tar_rolling_size < TARBALL_SIZE_LIMIT:
	    tar_rolling_size += file_size
            tmpfile.write()
	else:
	    execute_tar(pid, path)
            tar_rolling_size = 0

def get_program_arguments():
    parser = argparse.ArgumentParser(description='Transfers a directory from a remote host(s) in parallel to a given local filesystem using rsync')
    parser.add_argument('file_info_f', type=str, help='File containing one file path on the remote host per line')
    parser.add_argument('--num-procs', type=int, default=1, help='Number of procs to divy up lines .')
    args = parser.parse_args()
    return args

def main():
    args = get_program_arguments()
    logstr = f'Executing tar operation with the following parameters:\n'
    logger.info(logstr)

    procs = []
    tar_queue = Queue(args.num_procs)
    logger.info(f'Starting {args.num_procs} procs')
    for i in range(args.num_procs):
        p = Process(target=do_processing, args=(i, tar_queue, args))
        p.start()
        procs.append(p)

    # See if there is a point in the input file we should resume at
    start_from = 0
    fmarkfile_path = f'{args.file_info_f}.resume'
    try:
        logger.info(f'Checking {fmarkfile_path} for a line offset...')
        with open(fmarkfile_path, 'r') as fmarkfile:
            start_from = int(fmarkfile.read())
    except OSError:
        pass
    logger.info(f'Starting processing from {start_from} lines into the input file...')

    with open(args.transfer_info_f) as f:
        for i, item in enumerate(f):
            if i < start_from:
                if i < 10 or i % 500 == 0:
                    logger.info(f'Scrolling to where we left off...')
                continue
            tar_item = item.strip()
            tar_queue.put(tar_item)
            if i % 500 == 0:
                with open(fmarkfile_path, 'w') as fmarkfile_f:
                    fmarkfile_f.write(str(i))
            
    logger.info('All transfer items produced to consumer processes. Dispatching Sentinel.')
    for i in range(args.num_procs):
        tar_queue.put(Sentinel())
    for p in procs:
        p.join()

if __name__ == "__main__":
    main()
