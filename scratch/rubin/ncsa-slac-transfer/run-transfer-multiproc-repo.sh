#!/bin/bash

xfer_dest_dir=/sdf/group/rubin/scratch/ncsa-tarballs/repo-tarballs
pwd_f=/sdf/home/b/bjwhite/spellbook/scratch/rubin/ncsa-slac-transfer/ncsapwd.txt
host_list=gpfs12.ncsa.illinois.edu,gpfs13.ncsa.illinois.edu,gpfs14.ncsa.illinois.edu,gpfs15.ncsa.illinois.edu,gpfs16.ncsa.illinois.edu,gpfs17.ncsa.illinois.edu,gpfs18.ncsa.illinois.edu,gpfs19.ncsa.illinois.edu
parallel_rsync_script=/sdf/home/b/bjwhite/spellbook/general/parallel-rsync.py
echo $(date)
python3 ${parallel_rsync_script} \
	--user slacxfer \
	--num-procs ${1} \
        --ignore-checkpoint \
	${host_list} \
	${xfer_dest_dir} \
	${2} \
	${pwd_f}
echo $(date)
