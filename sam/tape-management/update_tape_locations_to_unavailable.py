# Given a list of files registered in SAM, set their tape locations to UNAVAILABLE
# Brandon White, 2021

import os
import sys
import pprint
import argparse
import threading
import collections
import time
import urllib2
import shlex
import re
import subprocess
import requests
import json
import samweb_client
import threading

def file_is_on_tape(experiment, filename, sam_location):
    prefix = 'https://fndca.fnal.gov:3880/api/v1/namespace/pnfs/fnal.gov/usr/%s' % experiment
    suffix = '?locality=true'
    full_path = sam_location['full_path'][18:] # Trim 'enstore:/pnfs/nova'
    url = prefix + full_path + '/' + filename + suffix
    resp = requests.get(url, verify=False)
    resp_d = json.loads(resp.text)
    try:
        on_tape = resp_d['fileLocality'] in ['NEARLINE', 'ONLINE_AND_NEARLINE']
    except KeyError:
        on_tape = False
    return on_tape 

def set_file_volume_unavailable(samweb, filename, sam_location):
    # Sets the tape location to unavailable in SAM
    body = {'label': 'unavailable', 'sequence': '-1'}
    url = 'https://samweb.fnal.gov:8483/sam/nova/api/files/name/%s/locations?location=%s' % ( filename, sam_location['full_path'] )
    resp = requests.put(url, data=body, verify='/etc/grid-security/certificates', cert='/tmp/x509up_u51660')
    if resp.status_code == 200:
        return True
    else:
        return False
    
def do_processing(tid, f, args):
    # Kick off set_file_volume_unavailble for each file if needed
    samweb = samweb_client.SAMWebClient(experiment=args.experiment)
    for filename in f:
        sam_locations = samweb.locateFile(filename)
        for sam_location in sam_locations:
            if sam_location['location_type'] == 'tape' and sam_location['location'].startswith('enstore:'):
                # check if the file is NEARLINE or ONLINE AND NEARLINE
                if file_is_on_tape(args.experiment, filename, sam_location):
                    # Set SAM tape status to "unavailable"
                    success = set_file_volume_unavailable(samweb, filename, sam_location)
                    print('Thread %s: %s' % (tid, filename))
                    
def get_file_queues(num_threads, f):
    # Splits the list of files into (almost) equal buckets of work per thread
    queues = [ [] for i in range(num_threads) ]
    i = 0
    for filename in f:
        which_queue = i % num_threads
        queues[which_queue].append(filename.strip())
        i += 1
    if num_threads == 1:
        return queues[0]
    return queues

def get_program_arguments():
    parser = argparse.ArgumentParser(description="Fix SAM tape locations for the provided list of files.")
    parser.add_argument('experiment', help='SAM Experiment')
    parser.add_argument('filelist', help='Text file with one file name per line of the files to be repaired.')
    args = parser.parse_args()
    return args

def main():
    args = get_program_arguments()
    threads = []
    num_threads = 8 
    with open(args.filelist) as f:
        file_queues =  get_file_queues(num_threads, f)
        for i in range(num_threads):
            t = threading.Thread(target=do_processing, args=(i, file_queues[i], args))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
