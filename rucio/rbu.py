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
    def __init__(self, args):
        self.dataset_name = args.dataset_name
        self.rucio_client = RucioClient(account=self.args.rucio_account)
        self.rucio_upload_client = RucioUploadClient(_client=self.rucio_client, logger=logger)


    def do_processing(self, tid, files, args):
        for f in files:
            #print(f'(tid {tid})f: {f}')
            # TODO: Upload the file
            rucio_upload(f)


    def rucio_create_dataset(self):
        account_arg = '-a {rucio_account}'.format(rucio_account=self.rucio_account)
        dataset_did = 'user.{rucio_account}:{dataset_name}'.format(rucio_account=self.rucio_account, dataset_name=dataset_name)
        cmd = 'rucio {account_arg} add-dataset {dataset_did}'\
            .format(account_arg=account_arg, dataset_did=dataset_did)
        logger.info(f'Running command: {cmd}')
        rucio_create_ds_proc = subprocess.run(cmd, shell=True)
        assert rucio_create_ds_proc.returncode == 0
        return dataset_did


    def rucio_upload(self, f):
        account_arg = '-a {rucio_account}'.format(rucio_account=self.rucio_account)
        rse_arg = '--rse {start_rse}'.format(start_rse=self.start_rse)
        cmd = 'rucio {account_arg} upload {rse_arg} {f}'\
            .format(account_arg=account_arg, rse_arg=rse_arg, filepath=filepath)
        logger.info(f'Running command: {cmd}')
        rucio_upload_proc = subprocess.run(cmd, shell=True)
        assert rucio_upload_proc.returncode == 0


    def rucio_attach_dataset(self, dataset_did, didfile_path):
        account_arg = '-a {rucio_account}'.format(rucio_account=self.rucio_account)
        didfile_arg = '-f {didfile_path}'.format(didfile_path=didfile_path)
        cmd = 'rucio {account_arg} attach {dataset_did} {didfile_arg}'\
            .format(account_arg=account_arg, dataset_did=dataset_did, didfile_arg=didfile_arg)
        logger.info(f'Running command: {cmd}')
        rucio_attach_ds_proc = subprocess.run(cmd, shell=True)
        assert rucio_attach_ds_proc.returncode == 0


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
    uploader = RucioUploader(args)
    uploader.rucio_create_dataset()

    threads = []
    with open(args.filelist) as f: # Obtain the work distribution and hand it to the threads
        file_queues =  get_file_queues(args.num_threads, f)
        for i in range(args.num_threads):
            t = threading.Thread(target=uploader.do_processing, args=(i, file_queues[i], args))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()


def get_program_arguments():
    parser = argparse.ArgumentParser(description="Rucio Bulk Upload: Helper to upload files to Rucio in parallel trivially.")
    parser.add_argument('rucio-account', help='Rucio account to be used.')
    parser.add_argument('dataset-did', help='Rucio formated DID (scope:name) of the dataset to be created that all uploaded files are to be attached to.')
    parser.add_argument('filelist', help='Text file with one file name per line of the files to be uploaded.')
    parser.add_argument('--num-threads', type=int, default=1, help='Number of threads to divy up filelist between.')

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
