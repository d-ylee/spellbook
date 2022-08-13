#!/bin/bash

# Print the dCache locality for all of the input files in PATHFILE
# MUST be absoulte paths.
# MUST be used on a host with dCache mounted for POSIX compatible access.
# Brandon White, 2022

PATHFILE=$1

while IFS= read -r line
do
	filepath=$(dirname "${line}")
	filename=$(basename "${line}")
	cmd="cat ${filepath}/\".(get)(${filename})(locality)\""
	locality=$(eval $cmd)
	out="File ${filename}: ${locality}"
	outstrip=$(echo $out | tr -d '\n')
	echo ${outstrip}
done < ${PATHFILE}
