# Parallel rsync Transfer Program
# Transfers a directory from remote host(s) in parallel to a given local filesystem using rsync
# Brandon White, 2022

import argparse
import getpass
import logging
import os
import os.path
import subprocess
import threading

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger()
fail_logger = None

def execute_transfer(tid, local_directory, remote_host, f_path, user, pwd_f):
    module = 'LSSTUser'
    remote_source = f'{user}@{remote_host}::{module}/{f_path}'
    format_string = '--out-format=\"%o %m %i %n %l %C\"'
    pwd_arg = f'--password-file={pwd_f}'
    logger.info(f'(tid:{tid}) Executing transfer of {f_path} from {remote_host} to {local_directory}\n\t\
            rsync --archive --relative {pwd_arg} {format_string} {remote_source} {local_directory}')
    # TODO: Is there a way to use ./ in the remote side for proper separation of the relative path on the destination side?
    xfer_process = subprocess.Popen([
        'rsync',
        '--archive',
        '--relative',
        '--verbose',
        '--progress',
        pwd_arg,
        format_string,
        remote_source,
        local_directory
    ], stdout=subprocess.PIPE)
    xfer_stdout, xfer_stderr = xfer_process.communicate()
    logger.info(f'(tid:{tid}) {xfer_stdout.decode().strip()}')
    if xfer_stderr is not None:
        if fail_logger is None:
            fail_logger = logging.getLogger('fail_log')
            fh = logging.FileHandler('rsync-fail-log')
            fail_logger.addHandler(fh)

        fail_logger.error(f'{xfer_stderr.decode().strip()}')

def do_processing(tid, files, args):
    remote_hosts = args.remotehosts.split(',')
    i = 0
    for f_path in files:
        remote_host_index = i % len(remote_hosts) # Incrementally select the next host round-robin for load-balancing
        execute_transfer(tid, args.localdirectory, remote_hosts[remote_host_index], f_path, args.user, args.password_file)
        i += 1

def get_file_queues(num_threads, f):
    # Splits the list of files into (almost) equal buckets of work per thread
    queues = [ [] for i in range(num_threads) ]
    i = 0
    for filename in f:
        which_queue = i % num_threads
        queues[which_queue].append(filename.strip())
        i += 1
    return queues

def get_program_arguments():
    parser = argparse.ArgumentParser(description='Transfers a directory from a remote host(s) in parallel to a given local filesystem using rsync')
    parser.add_argument('remotehosts', type=str, help='Remote hostname to transfer from. \
            May be provided as a comma-separated list of hostnames to cycle through.\n\
            All hostnames must have the same view of the transfer source filesystem.')
    parser.add_argument('localdirectory', type=str, help='Local directory that is the \
            destination of the transfer operation.')
    parser.add_argument('transfer_info_f', type=str, help='File containing one file path on the remote host per line')
    parser.add_argument('password_file', type=str, help='When running in daemon mode, you are gonna want this.')
    parser.add_argument('--num-threads', type=int, default=1, help='Number of threads to divy up lines .')
    parser.add_argument('--user', type=str, default=getpass.getuser(), help='User to execute the rsync as. \
            Defaults to current linux user.')

    args = parser.parse_args()
    return args

def main():
    args = get_program_arguments()
    logstr = f'Executing transfer with the following parameters:\n\
            \tRemote Host(s): {args.remotehosts}\n\
            \tLocal Directory (transfer destination): {args.localdirectory}\n\
            \tNum Threads: {args.num_threads}\n\
            \tUser: {args.user}'
    logger.info(logstr)

    if not os.path.isdir(args.localdirectory):
        os.mkdir(args.localdirectory)
    
    threads = []
    with open(args.transfer_info_f) as f: # Obtain the work distribution and hand it to the threads
        file_queues =  get_file_queues(args.num_threads, f)
        logger.info(f'Starting {args.num_threads} threads')
        for i in range(args.num_threads):
            t = threading.Thread(target=do_processing, args=(i, file_queues[i], args))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
