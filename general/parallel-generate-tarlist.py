# tar tvf Program
# Given an input file consisting of paths to tar archives (can be relative) 
#      create a per-archive listing of the files contained therin 
# Brandon White, 2022

import argparse
import getpass
import hashlib
import logging
import math
import os
import os.path
import pathlib
import shutil
import subprocess
import tempfile
from multiprocessing import Queue

from util import Sentinel, start_processes, end_processes, get_start_offset,\
        set_logger, at_offset, write_offset_file

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger()

def execute_listing(pid, archive_path, listing_dest_path, fail_logger):
    logger.info(f'(pid:{pid}) Executing listing of files specified in {archive_path} and sending to file {listing_dest_path}')
    with open(listing_dest_path, 'w') as final_listing_file:
        logger.info(f'listing_dest_path: listing_dest_path')
        tar_process = subprocess.Popen([
            'tar',
            '--list',
            '--verbose',
            f'--file={archive_path}',
        ], stdout=subprocess.PIPE)
        # We only need the file size and path for our purposes
        awk_process = subprocess.Popen(
            ['awk', '{print $3"    "$6}'],
            stdin=tar_process.stdout,
            stdout=final_listing_file
        )
        listing_stdout, listing_stderr = awk_process.communicate()
    if awk_process.returncode != 0:
        logger.info(f'!!! AWK FAILURE: Failed to include file {archive_path} in archive {listing_dest_path} !!!')
        fail_logger.error(archive_path.encode(encoding='UTF-8'))
    logger.info(f'(pid:{pid}): TAR LISTING COMPLETE: {archive_path}')

def do_processing(pid, list_queue, args):
    while True:
        archive_path = list_queue.get()
        if isinstance(archive_path, Sentinel):
            logger.info(f'PID: {pid} complete. Waiting to join.')
            return

        # Create a path for storing the per-archive listings 
        listing_dest_path = os.path.join(
                args.listing_dest_dir,
                os.path.basename(archive_path) + '.listing') # path of final archive listing for each file output by the program
        # Setup error logging
        fail_log_path = listing_dest_path + '.error' # Name of per-archive failure logs
        fail_logger = logging.getLogger('fail_log')
        fh = logging.FileHandler(fail_log_path)
        fail_logger.addHandler(fh)
        # Do the thing
        execute_listing(pid, archive_path, listing_dest_path, fail_logger) # DO THE TAR

def get_program_arguments():
    parser = argparse.ArgumentParser(description='Transfers a directory from a remote host(s) in parallel to a given local filesystem using rsync')
    parser.add_argument('listing_info_f', type=str, help='File containing one file path on the local host per line')
    parser.add_argument('--num-procs', type=int, default=1, help='Number of procs to divy up lines .')
    parser.add_argument('--listing-prefix', type=str, default='tarlist_', help='Name to prepend to files used to accumulate the per-archive file listings.')
    parser.add_argument('--listing-dest-dir', type=str, default='/tmp', help='Number of procs to divy up lines.')
    parser.add_argument('--ignore-checkpoint', default=False, action='store_true')
    args = parser.parse_args()
    return args

def main():
    set_logger(logger)
    args = get_program_arguments()
    logstr = f'Executing tar listing operation for input archive set'
    logger.info(logstr)

    list_queue = Queue(args.num_procs)
    logger.info(f'Starting {args.num_procs} procs')
    procs = start_processes(list_queue, do_processing, args.num_procs, args)

    # See if there is a point in the input file we should resume at
    start_from, offset_file_path = get_start_offset(args.listing_info_f, args.ignore_checkpoint)
    logger.info(f'Starting processing from {start_from} lines into the input file...')

    with open(args.listing_info_f) as f:
        for i, item in enumerate(f):
            while not at_offset(i, start_from, f):
                continue
            list_item = item.strip()
            list_queue.put(list_item)
            write_offset_file(i, offset_file_path) # TODO: should probably do after the tar completes instead
            
    logger.info('All transfer items produced to consumer processes. Dispatching Sentinel.')
    end_processes(list_queue, procs)

if __name__ == "__main__":
    main()
