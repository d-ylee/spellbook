#!/bin/bash

parallel_tar_script=/sdf/home/b/bjwhite/spellbook/general/parallel-tar.py
echo $(date)
python3 ${parallel_tar_script} \
	${1} \ # File with "filesize filepath" format
	--num-procs ${2} \
	--tar-dir ${3}
	#--tar-prefix ${4} \
echo $(date)
