#!/usr/bin/env python3
import sys
import os
from rucio.client.replicaclient import ReplicaClient
from rucio.client.didclient import DIDClient
import rucio.rse.rsemanager as rsemgr

mydataset = 'Test100000'
scope = 'root.dc2dr6'
myrse = 'NCSATEST4_DATADISK'
R = ReplicaClient()
D = DIDClient()
infile = sys.argv[1]
preUrl = 'https://lsst-dbb-gw02.ncsa.illinois.edu:1094/'
count = 0
replicaList = []
with open(infile) as fp:
    while True:
        count += 1
        line = fp.readline()
        if not line:
            break
        sline = line.strip()
        data = sline.split(' ')
        dcount = 0
        for i in data:
            dcount += 1
            if (dcount == 1) :
                path = i
                pdata = path.split('/')
            if (dcount == 2) :
                checksum = i
            if (dcount == 3) :
                nbytes = int(i)
        pfn = preUrl + path
        name = pdata[-1]
        # print(scope) # print(pfn) # print(checksum) # print(nbytes) # print(” “)
        print(scope, name, checksum)
        REPLICA = {
             'scope': scope,
             'name' : name,
             'adler32': checksum,
             'bytes': nbytes,
             'pfn': pfn
        }
        replicaList.append(REPLICA)
R.add_replicas(rse=myrse, files=replicaList)
D.attach_dids(scope, mydataset, replicaList) 
