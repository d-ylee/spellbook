# tar tvf Program
# Given an input file consisting of paths to tar archives, untar them into a given directory
# They will assume a relative path below this directory, replicating the layout at the destination
# Brandon White, 2022

import argparse
import logging
import os.path
import subprocess
from multiprocessing import Queue

from util import Sentinel, start_processes, end_processes, get_start_offset,\
        set_logger, at_offset, write_offset_file

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger()

def execute_untar(pid, archive_path, untar_dest_path, fail_logger):
    logger.info(f'(pid:{pid}) Executing extraction of files specified in {archive_path} and sending to file {untar_dest_path}')
    logger.info(f'tar --extract --verbose --file={archive_path} --directory={untar_dest_path}')
    tar_process = subprocess.Popen([
        'tar',
        '--extract',
        '--verbose',
        '--keep-directory-symlink',
        '--keep-newer-files',
        f'--file={archive_path}',
        f'--directory={untar_dest_path}',
    ], stdout=subprocess.PIPE)
    tar_stdout, tar_stderr = tar_process.communicate()
    if tar_process.returncode != 0:
        logger.info(f'!!! TAR FAILURE: Failed to include file {archive_path} in archive {untar_dest_path} !!!')
        fail_logger.error(archive_path.encode(encoding='UTF-8'))
    logger.info(f'(pid:{pid}): TAR EXTRACTION COMPLETE: {archive_path}')

def do_processing(pid, archive_queue, args):
    while True:
        archive_path = archive_queue.get()
        if isinstance(archive_path, Sentinel):
            logger.info(f'PID: {pid} complete. Waiting to join.')
            return

        untar_dest_path = args.dest_dir
        # Setup error logging
        fail_log_path = untar_dest_path + '.error' # Name of per-archive failure logs
        fail_logger = logging.getLogger('fail_log')
        fh = logging.FileHandler(fail_log_path)
        fail_logger.addHandler(fh)
        # Do the thing
        execute_untar(pid, archive_path, untar_dest_path, fail_logger) # DO THE TAR

def get_program_arguments():
    parser = argparse.ArgumentParser(description='Transfers a directory from a remote host(s) in parallel to a given local filesystem using rsync')
    parser.add_argument('tarlist_info_f', type=str, help='File containing one file path on the local host per line')
    parser.add_argument('--num-procs', type=int, default=1, help='Number of procs to divy up lines .')
    parser.add_argument('--dest-dir', type=str, default='/tmp', help='Destination within which the untarred archives will be placed.')
    parser.add_argument('--ignore-checkpoint', default=False, action='store_true')
    args = parser.parse_args()
    return args

def main():
    set_logger(logger)
    args = get_program_arguments()
    logstr = f'Executing untar operation for input archive set'
    logger.info(logstr)

    archive_queue = Queue(args.num_procs)
    #completed_queue = Queue() # TODO
    #failed_queue = Queue() # TODO
    logger.info(f'Starting {args.num_procs} procs')
    procs = start_processes(archive_queue, do_processing, args.num_procs, args) # TODO: Start with the completed/failed queues

    # See if there is a point in the input file we should resume at
    start_from, offset_file_path = get_start_offset(args.tarlist_info_f, args.ignore_checkpoint)
    logger.info(f'Starting processing from {start_from} lines into the input file...')

    with open(args.tarlist_info_f) as f:
        for i, item in enumerate(f):
            while not at_offset(i, start_from, f):
                continue
            archive_item = item.strip()
            archive_queue.put(archive_item)
            # TODO: Check the operation result queue to see if any files were completed, if so, then run_write_offset_file
            write_offset_file(i, offset_file_path) # TODO: should probably do after the tar completes instead
            
    logger.info('All items produced to consumer processes. Dispatching Sentinel.')
    end_processes(archive_queue, procs)

if __name__ == "__main__":
    main()
