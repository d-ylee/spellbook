#!/bin/bash

parallel_untar_script=/home/bjwhite/spellbook/general/parallel-untar.py
echo $(date)
python3 ${parallel_untar_script} \
	--num-procs ${1} \
	--dest-dir ${2} \
        --ignore-checkpoint \
	${3} # File with one file path to a tar archive per line 
echo $(date)
