# Rucio Bulk Upload (rbu)
# Given a list of files, upload them to Rucio
# Brandon White, 2022

import argparse
import subprocess
import threading
import rucio
import logger


logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger('rbu')


class RucioUploader:
    def __init__(self, rucio_account, account, scope, dataset_name, rse):
        self.rucio_account = rucio_account
        self.scope = scope
        self.dataset_name = dataset_name
        self.rse = rse 
        self.rucio_client = RucioClient(account=self.rucio_account)
        self.rucio_upload_client = RucioUploadClient(_client=self.rucio_client, logger=logger)

    def do_processing(self, tid, files):
        upload_items = prepare_items(files)
        self.rucio_upload_client.upload(items)

    def prepare_items(self, files):
        items = []
        for f in files:
            item = {
                'path': f,
                'rse': self.start_rse,
                'did_scope': self.rucio_scope,
                'dataset_scope': self.rucio_scope,
                'dataset_name': self.dataset_name,
            }
            items.append(item)
        return items

    def rucio_create_dataset(self):
        logger.info(f'Creating Rucio dataset')
        rc = self.rucio_client.add_dataset(
                self.test_params.rucio_scope,
                dataset_name,
                rse=self.rse
        )
        return f'{self.scope}:{self.dataset_name}'


def get_file_queues(num_threads, f):
    # Splits the list of files into (almost) equal buckets of work per thread
    queues = [ [] for i in range(num_threads) ]
    i = 0
    for filename in f:
        which_queue = i % num_threads
        queues[which_queue].append(filename.strip())
        i += 1
    return queues


def main():
    args = get_program_arguments()
    uploader = RucioUploader(args.rucio_account, args.scope, args.dataset_name, args.rse)
    uploader.rucio_create_dataset()

    threads = []
    with open(args.filelist) as f: # Obtain the work distribution and hand it to the threads
        file_queues =  get_file_queues(args.num_threads, f)
        for i in range(args.num_threads):
            t = threading.Thread(target=uploader.do_processing, args=(i, file_queues[i]))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()


def get_program_arguments():
    parser = argparse.ArgumentParser(description="Rucio Bulk Upload: Helper to upload files to Rucio in parallel trivially.")
    parser.add_argument('rucio-account', help='Rucio account to be used.')
    parser.add_argument('dataset-name', help='Name of the dataset to be created that all uploaded files are to be attached to.')
    parser.add_argument('rse', help='Rucio Storage Element that the files will be uploaded to.')
    parser.add_argument('filelist', help='Text file with one file name per line of the files to be uploaded.')
    parser.add_argument('--num-threads', type=int, default=1, help='Number of threads to divy up filelist between.')
    parser.add_argument('--scope', help='Rucio scope that the files are to be placed in. Default: user.{rucio-account}')

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
