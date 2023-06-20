# Parallel rsync Transfer Program
# Transfers files/directories from remote rsync servers load-balanced
#     round-robin in parallel to a local filesystem using rsync
# Brandon White, 2022

# Note: This will currently only with with an rsync daemon running on the remote end.
# See the definition of execute_transfer(), as it used :: in building the source
# transfer resource locator (remote_source)

import argparse
import getpass
import logging
import os
import os.path
import subprocess
from multiprocessing import Queue

from util import Sentinel, start_processes, end_processes, get_start_offset, set_logger, at_offset, write_offset_file

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger()

def execute_transfer(pid, local_directory, remote_host, transfer_path, user, pwd_f, fail_logger):
    #module = 'LSSTUser' # TODO: Make this an actual argument
    #module = 'LSSTScratch' # TODO: Make this an actual argument
    #module = 'LSSTScratch' # TODO: Make this an actual argument
    #module = 'LSSTTarballs'
    module = 'LSSTktl'
    remote_source = f'{user}@{remote_host}::{module}/{transfer_path}'
    format_string = '--out-format=\"%o %m %i %n %l %C\"' # TODO: Make this an actual argument
    pwd_arg = f'--password-file={pwd_f}'
    logger.info(f'(pid:{pid}) Executing transfer of {transfer_path} from {remote_host} to {local_directory}\n\t\
            rsync --archive --remove-source-files --xattrs {pwd_arg} {format_string} {remote_source} {local_directory}')
    xfer_process = subprocess.Popen([
        'rsync',
        '--archive',
        '--remove-source-files',
        #'--relative',
        '--xattrs',
        pwd_arg,
        format_string,
        remote_source,
        local_directory
    ], stdout=subprocess.PIPE)
    xfer_stdout, xfer_stderr = xfer_process.communicate()
    logger.info(f'(pid:{pid}) {xfer_stdout.decode().strip()}')
    if xfer_process.returncode != 0:
        logger.info(f'!!!  TRANSFER FAILURE: {remote_source}')
        fail_logger.error(transfer_path)

def do_processing(pid, transfer_queue, args):
    fail_log_path = os.path.join(args.fail_log_path, f'{os.path.basename(args.transfer_info_f)}.{pid}.error') 
    fail_logger = logging.getLogger('fail_log')
    fh = logging.FileHandler(fail_log_path)
    fail_logger.addHandler(fh)

    remote_hosts = args.remotehosts.split(',')
    i = 0
    while True:
        transfer_path = transfer_queue.get()
        if isinstance(transfer_path, Sentinel):
            logger.info(f'PID: {pid} complete. Waiting to join.')
            return
        remote_host_index = i % len(remote_hosts) # Incrementally select the next host round-robin for load-balancing
        execute_transfer(pid, args.localdirectory, remote_hosts[remote_host_index], transfer_path, args.user, args.password_file, fail_logger)
        i += 1

def get_program_arguments():
    parser = argparse.ArgumentParser(description='Transfers a directory from a remote host(s) in parallel to a given local filesystem using rsync')
    parser.add_argument('remotehosts', type=str, help='Remote hostname to transfer from. \
            May be provided as a comma-separated list of hostnames to cycle through.\n\
            All hostnames must have the same view of the transfer source filesystem.')
    parser.add_argument('localdirectory', type=str, help='Local directory that is the \
            destination of the transfer operation.')
    parser.add_argument('transfer_info_f', type=str, help='File containing one file path on the remote host per line')
    parser.add_argument('password_file', type=str, help='When running in daemon mode, you are gonna want this.')
    parser.add_argument('--num-procs', type=int, default=1, help='Number of procs to divy up lines .')
    parser.add_argument('--ignore-checkpoint', default=False, action='store_true')
    parser.add_argument('--user', type=str, default=getpass.getuser(), help='User to execute the rsync as. \
            Defaults to current linux user.')
    parser.add_argument('--fail-log-path', type=str, default=f'/sdf/group/rubin/scratch/transfer_lists/error_files',
            help='User to execute the rsync as. Defaults to current linux user.')

    args = parser.parse_args()
    return args

def main():
    set_logger(logger)
    args = get_program_arguments()
    logstr = f'Executing transfer with the following parameters:\n\
            \tRemote Host(s): {args.remotehosts}\n\
            \tLocal Directory (transfer destination): {args.localdirectory}\n\
            \tNum Procs: {args.num_procs}\n\
            \tUser: {args.user}'
    logger.info(logstr)

    if not os.path.isdir(args.localdirectory):
        os.mkdir(args.localdirectory)
    
    transfer_queue = Queue(args.num_procs)
    logger.info(f'Starting {args.num_procs} procs')
    procs = start_processes(transfer_queue, do_processing, args.num_procs, args)

    # See if there is a point in the input file we should resume at
    start_from, offset_file_path = get_start_offset(args.transfer_info_f, args.ignore_checkpoint)
    logger.info(f'Starting processing from {start_from} lines into the input file...')

    with open(args.transfer_info_f) as f: # Obtain the work distribution and hand it to the procs
        for i, item in enumerate(f):
            while not at_offset(i, start_from, f):
                continue
            transfer_item = item.strip()
            transfer_queue.put(transfer_item)
            write_offset_file(i, offset_file_path)
            
    logger.info('All transfer items produced to consumer processes. Dispatching Sentinel.')
    end_processes(transfer_queue, procs)

if __name__ == "__main__":
    main()
