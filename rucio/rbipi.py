#!/usr/bin/env python3

# Rucio Bulk In-Place Ingest

# PFNs will be determined on the Rucio side automatically for DETERMINISTIC storage endpoints.
# use ndrseipi.py for nondeterministic storage elements

import argparse
import logging
import os
import sys
from time import sleep
import multiprocessing as mp

from rucio.client import Client as RucioClient

from rucio.client.replicaclient import ReplicaClient
from rucio.client.didclient import DIDClient
from rucio.common.exception import DataIdentifierAlreadyExists
import rucio.rse.rsemanager as rsemgr

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger('rbipi')

class Registrar:
    def __init__(self, args):
        self.just_say = args.just_say
        self.rucio_account = args.rucio_account
        self.scope = args.scope if args.scope is not None else f'user.{self.rucio_account}'
        self.dataset_name = args.dataset_name
        self.rse = args.rse

    def do_processing(self, tid, files, R, D, badlist):
        logger.info(f'(tid:{tid}) There are {len(files)} LFNs to process.')
        registration_items, contents  = self.prepare_items(files, D, badlist)
        if not self.just_say:
            logger.info(f'(tid:{tid}) Registering {len(registration_items)} LFNs to {self.rse}.')
            if len(registration_items) > 0 :
                logger.info(f'Adding them to the dataset {self.scope}:{self.dataset_name}')
                try:
                    R.add_replicas(rse=self.rse, files=registration_items)
                    attachments=[{'scope': self.scope, 'name': self.dataset_name, 'dids':contents }]
                    D.add_files_to_datasets(attachments, ignore_duplicate=True)
                    logger.info(f'ingested {len(registration_items)} LFNs.\n\t ALL DONE!')
                except Exception as ex:
                    logger.error(ex) 
                    logger.info("First registering to REPLICAS and DIDS failed! Will try again after 0.05 second.")
                    sleep(0.05)
                    logger.info("Second try to register to REPLICAS and DIDS!" )
                    R.add_replicas(rse=self.rse, files=registration_items)
                    attachments=[{'scope': self.scope, 'name': self.dataset_name, 'dids':contents }]
                    D.add_files_to_datasets(attachments, ignore_duplicate=True)
                    logger.info(f'ingested {len(registration_items)} LFNs.\n\t ALL DONE!')
            else:
                logger.info("No LFNs registered, They already in Rucio. \n\t ALL DONE!")
        else:
            logger.info(f'(tid:{tid}) Would have registered {len(registration_items)} LFNs to {self.rse}.\
                    \n\tWould have added them to the dataset {self.scope}:{self.dataset_name}')

    def prepare_items(self, files, D, badlist):
        items = []
        contents = []
        count = 0
        try:
            ds = list(D.list_content(self.scope, self.dataset_name))
        except Exception as ex:
            logger.error(ex) 
            logger.error("First try to connect to Rucio failed ! Will try again after 0.05 second.")
            sleep(0.05)
            ds = list(D.list_content(self.scope, self.dataset_name))
            logger.info("Sucessfully connected to Rucio! Get contents from Rucio.")
        bad =  open(badlist, 'a')
        np = 0
        for fileinfo_raw in files:
            try:
                fileinfo = fileinfo_raw.split(" ")
                name = fileinfo[0]
                adler = fileinfo[1]
#               md5 = fileinfo[2]
                nbytes = int(fileinfo[2])
                np += 1
                if np < 3 :
                    logger.info(f'Create new item for registration: {self.scope}, {name}, {adler}, {nbytes}')
                replica = {
                 'scope': self.scope,
                 'name' : name,
                 'bytes': nbytes,
                 'adler32': adler,
#                'md5': md5,
                }
                if {'scope': self.scope, 'name': name, 'type': 'FILE', 'bytes': nbytes, 'adler32': adler, 'md5': None} not in ds:
                    items.append(replica)
                    contents.append({'scope': self.scope, 'name': name})
            except IndexError:
                bad.write(fileinfo_raw + '\n')
                pass
        bad.close()
        return items, contents

def get_file_queues(num_procs, f):
    # Splits the list of files into (almost) equal buckets of work per proc
    queues = [ [] for i in range(num_procs) ]
    num_files = 0
    for filename in f:
        which_queue = num_files % num_procs
        queues[which_queue].append(filename.strip())
        num_files += 1
    logger.info(f'(Main) Total LFNs: {num_files}\n\t~{num_files/num_procs} allocated per worker')
    return queues

def main():
    args = get_program_arguments()
    registrar = Registrar(args)

    procs = []
    logger.info(f'(Main) Starting up {args.num_procs} worker processes')
    registrar.rucio_client = RucioClient(account=args.rucio_account)
    R = ReplicaClient()
    D = DIDClient()
    logger.info(f'Creating dataset {args.scope}:{args.dataset_name}')
    try:
        D.add_did(
                scope=args.scope if args.scope is not None else f'user.{args.rucio_account}',
                name=args.dataset_name,
                did_type='dataset',
                rules=[
                    { 'account': args.rucio_account, 'copies': 1, 'rse_expression': args.rse }
                ]
        )
    except DataIdentifierAlreadyExists:
        pass # This is fine, we might want to add more files to the same dataset

    path = '.'
    for fl in os.listdir(path):
        if args.filelist in fl :
            badlist = fl + '.bad'
            logger.info(f'(Main)Starting to process file: {fl}')
            with open(fl) as f: # Obtain the work distribution and hand it to the procs
                file_queues =  get_file_queues(args.num_procs, f)
                for i in range(args.num_procs):
                    p = mp.Process(target=registrar.do_processing, args=(i, file_queues[i], R, D, badlist))
                    p.start()
                    procs.append(p)
            logger.info(f'(Main) Processing file:  {fl}.')
        for t in procs:
            t.join()

def get_program_arguments():
    parser = argparse.ArgumentParser(description='Rucio Bulk In-Place Ingest: Register files with the Rucio DB without transferring them.')
    parser.add_argument('dataset_name', help='Name of the dataset to be created that all ingested files are to be attached to.')
    parser.add_argument('rse', help='Rucio Storage Element that the files will be ingested to.')
    parser.add_argument('filelist', help='Text file name in the directoy with information for one file per line of the files to be registered.\\n\tLine Format: <name> <Adler32 checksum> <size in bytes>. \\n\tIf one want to covery all the files, one can use partial name. Ex: if one wants to ingest all datafile[0-9][0-9].txt, the filelist will be datafile.\\n\tThe data should be placed at a location in the RSE dependent on the MD5 checksum of the scope:name LFN. The Adler32 used in this --filelist argument concerns long-term data verification rather than data location.')
    parser.add_argument('--num-procs', type=int, default=1, help='Number of processes to divy up filelist between.')
    parser.add_argument('--rucio-account', default="root", help='Rucio account to be used.')
    parser.add_argument('--scope', help='Rucio scope that the files are to be placed in. Default: user.{rucio-account}')
    parser.add_argument('--just-say', type=bool, default=False, help='For testing. Do not actually ingest files if True. Default: False')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
