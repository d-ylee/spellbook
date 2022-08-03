#!/bin/bash

parallel_tar_script=/home/bjwhite/spellbook/general/parallel-tar.py
echo $(date)
python3 ${parallel_tar_script} \
	--num-procs ${1} \
	--tar-dest-dir ${2} \
	${3} # File with "filesize filepath" format
	#--tar-prefix ${4} \
echo $(date)
