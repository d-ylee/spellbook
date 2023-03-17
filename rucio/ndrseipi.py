#!/usr/bin/env python3

# Ingests files for rucio non-deterministic rse


import argparse
import copy
import json
import logging
import time
import random

import gfal2
from rucio.client import Client as RucioClient
from rucio.common.config import config_get_int, config_get
from rucio.common.exception import (RucioException, RSEWriteBlocked, DataIdentifierAlreadyExists, RSEOperationNotSupported,
                                    DataIdentifierNotFound, NoFilesUploaded, NotAllFilesUploaded, FileReplicaAlreadyExists,
                                    ResourceTemporaryUnavailable, ServiceUnavailable, InputValidationError, RSEChecksumUnavailable,
                                    ScopeNotFound)
from rucio.client.uploadclient import UploadClient
from rucio.common.utils import (adler32, detect_client_location, execute, generate_uuid, make_valid_did, md5, send_trace,
                                retry, GLOBALLY_SUPPORTED_CHECKSUMS)
from rucio.rse import rsemanager as rsemgr

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger('rbipi')


class InPlaceIngestClient(UploadClient):
    def __init__(self, _client=None, logger=None, tracing=True, ctxt=None):
        super().__init__(_client, logger, tracing)
        self.ctxt = ctxt

    def _upload_item(self, rse_settings, rse_attributes, lfn,
                     source_dir=None, domain='wan', impl=None,
                     force_pfn=None, force_scheme=None, transfer_timeout=None,
                     delete_existing=False, sign_service=None) -> str:
        """Override _upload_item"""
        pfn = force_pfn
        return pfn

    def _collect_file_info(self, filepath, item):
        """
        Collects infos (e.g. size, checksums, etc.) about the file and
        returns them as a dictionary
        (This function is meant to be used as class internal only)

        :param filepath: path where the file is stored
        :param item: input options for the given file

        :returns: a dictionary containing all collected info and the input options
        """
        new_item = copy.deepcopy(item)
        new_item['path'] = filepath
        new_item['dirname'] = filepath
        new_item['basename'] = filepath.split('/')[-1]

        file_stats = self.ctxt.lstat(filepath)

        new_item['bytes'] = file_stats.st_size

        adler32 = self.ctxt.checksum(filepath, 'adler32')
        new_item['adler32'] = adler32

        md5 = self.ctxt.checksum(filepath, 'md5')
        new_item['md5'] = md5
        new_item['meta'] = {'guid': self._get_file_guid(new_item)}
        new_item['state'] = 'C'
        if not new_item.get('did_scope'):
            new_item['did_scope'] = self.default_file_scope
        if not new_item.get('did_name'):
            new_item['did_name'] = new_item['basename']

        return new_item

    def _collect_and_validate_file_info(self, items):
        """
        Checks if there are any inconsistencies within the given input
        options and stores the output of _collect_file_info for every file
        (This function is meant to be used as class internal only)

        :param filepath: list of dictionaries with all input files and options

        :returns: a list of dictionaries containing all descriptions of the files to upload

        :raises InputValidationError: if an input option has a wrong format
        """
        logger = self.logger
        files = []
        for item in items:
            path = item.get('path')
            pfn = item.get('pfn')

            if not path:
                path = pfn
            if not item.get('rse'):
                logger(logging.WARNING, 'Skipping file %s because no rse was given' % path)
                continue
            if pfn:
                item['force_scheme'] = pfn.split(':')[0]
            if item.get('impl'):
                impl = item.get('impl')
                impl_split = impl.split('.')
                if len(impl_split) == 1:
                    impl = 'rucio.rse.protocols.' + impl + '.Default'
                else:
                    impl = 'rucio.rse.protocols.' + impl
                item['impl'] = impl
            file = self._collect_file_info(path, item)
            files.append(file)

        return files


def discover_files(ctxt, rse: str, directory: str, scope: str) -> list:
    '''Discover files on the server to be ingested
    '''
    files = ctxt.listdir(directory)

    items = []
    for f in files:
        name = f
        pfn = f'{directory}/{f}'
        f_stat = ctxt.lstat(pfn)
        size = f_stat.st_size
        adler32 = ctxt.checksum(pfn, 'adler32')

        replica = {
            'name': name,
            'scope': scope,
            'bytes': size,
            'adler32': adler32,
            'path': pfn,
            'pfn': pfn,
            'rse': rse,
            'register_after_upload': True
        }
        items.append(replica)

    return items


def inplace_ingest(target_dir, rse):
    ctxt = gfal2.creat_context()

    rucio_client = RucioClient(account='dylee')
    inplace_ingest_client = InPlaceIngestClient(rucio_client, logger=logger, ctxt=ctxt)

    rse_info = rucio_client.get_rse(rse=rse)
    rse_attributes = rucio_client.list_rse_attributes(rse)
    print(rse_attributes)
    print(rse_info)
    protocol = target_dir.split(":")[0]
    print([p['prefix'] for p in rse_info["protocols"] if p['scheme'] == protocol])

    items = discover_files(ctxt, rse, target_dir, 'user.dylee')

    inplace_ingest_client.upload(items)


def main():
    args = get_program_arguments()
    target_dir = args.file_directory
    rse = args.rse
    inplace_ingest(target_dir, rse)

def get_program_arguments():
    parser = argparse.ArgumentParser(
        description='''Rucio Ingest: scans for existing files
        using gfal2 and register into a non-deterministic RSE without copying''')
    parser.add_argument(
        'file_directory',
        help='Full URI of the target directory i.e. root://<server>:<port>/<path>')
    parser.add_argument(
        'rse',
        help='Rucio Storage Element that the files will be ingested to.'
    )

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()