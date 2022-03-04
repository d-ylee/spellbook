# Rucio Bulk Upload (rbu)
# Given a list of files, upload them to Rucio
# Brandon White, 2022

import argparse
import logging
import os
import rucio
import subprocess
import threading

from rucio.client import Client as RucioClient
from rucio.client.uploadclient import UploadClient as RucioUploadClient


logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger('rbu')


class RucioUploader:
    def __init__(self, args):
        self.just_say = args.just_say
        self.rucio_account = args.rucio_account
        self.scope = args.scope
        self.dataset_name = args.dataset_name
        self.rse = args.rse 
        self.register_after_upload = args.register_after_upload
        self.rucio_client = RucioClient(account=self.rucio_account)
        self.rucio_upload_client = RucioUploadClient(_client=self.rucio_client, logger=logger)

    def do_processing(self, tid, files):
        logger.info(f'(tid:{tid}) Preparing {len(files)} files for upload')
        upload_items = prepare_items(files)
        if not self.just_say:
            logger.info(f'(tid:{tid}) Uploading {len(files)} files to {self.rse}.\n\tAdding them to the dataset {self.scope}:{self.dataset_name}')
            self.rucio_upload_client.upload(items)
        else:
            logger.info(f'(tid:{tid}) Would have uploaded {len(files)} files to {self.rse}.\n\tWould have added them to the dataset {self.scope}:{self.dataset_name}')


    def prepare_items(self, files):
        items = []
        for f in files:
            item = {
                'path': f,
                'rse': self.start_rse,
                'did_scope': self.rucio_scope,
                'dataset_scope': self.rucio_scope,
                'dataset_name': self.dataset_name,
                'register_after_upload': self.register_after_upload
            }
            items.append(item)
        return items

    def rucio_create_dataset(self):
        logger.info(f'Creating Rucio dataset')
        if not self.just_say:
            logger.info(f'(Main) Creating dataset {self.scope}:{self.dataset_name}')
            rc = self.rucio_client.add_dataset(
                    self.test_params.rucio_scope,
                    dataset_name,
                    rse=self.rse
            )
        else:
            logger.info(f'(Main) Would created the dataset {self.scope}:{self.dataset_name}')


def get_file_queues(num_threads, f):
    # Splits the list of files into (almost) equal buckets of work per thread
    queues = [ [] for i in range(num_threads) ]
    num_files = 0
    for filename in f:
        which_queue = num_files % num_threads
        queues[which_queue].append(filename.strip())
        num_files += 1
    logger.info(f'(Main) Total files: {num_files}\n\t~{num_files/num_threads} allocated per worker')
    return queues


def main():
    args = get_program_arguments()
    uploader = RucioUploader(args)
    uploader.rucio_create_dataset()

    threads = []
    logger.info(f'(Main) Starting up {args.num_threads} worker threads')
    with open(args.filelist) as f: # Obtain the work distribution and hand it to the threads
        file_queues =  get_file_queues(args.num_threads, f)
        for i in range(args.num_threads):
            t = threading.Thread(target=uploader.do_processing, args=(i, file_queues[i]))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()
    logger.info(f'(Main) Uploads complete.')


def get_program_arguments():
    parser = argparse.ArgumentParser(description="Rucio Bulk Upload: Helper to upload files to Rucio in parallel trivially.")
    parser.add_argument('dataset_name', help='Name of the dataset to be created that all uploaded files are to be attached to.')
    parser.add_argument('rse', help='Rucio Storage Element that the files will be uploaded to.')
    parser.add_argument('filelist', help='Text file with one file name per line of the files to be uploaded.')
    parser.add_argument('--num-threads', type=int, default=1, help='Number of threads to divy up filelist between.')
    parser.add_argument('--rucio-account', default=os.getlogin(), help='Rucio account to be used.')
    parser.add_argument('--scope', help='Rucio scope that the files are to be placed in. Default: user.{rucio-account}')
    parser.add_argument('--register-after-upload', type=bool, default=False, help='Passed to Rucio upload(). Default: False')
    parser.add_argument('--just-say', type=bool, default=False, help='For testing. Do not actually upload files if True. Default: False')

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
