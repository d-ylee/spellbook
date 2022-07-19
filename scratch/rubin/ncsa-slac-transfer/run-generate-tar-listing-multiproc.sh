#!/bin/bash

parallel_listing_script=/home/bjwhite/spellbook/general/parallel-generate-tarlist.py
echo $(date)
python3 ${parallel_listing_script} \
	--num-procs ${1} \
	--listing-dest-dir ${2} \
        --ignore-checkpoint \
	${3} # File with one file path to a tar archive per line 
echo $(date)
