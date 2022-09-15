#!/bin/bash

# Print the dCache locality for all of the input files in PATHFILE
# DO use input paths of the form <experiment>/*
# You MUST NOT include a leading slash
# DO NOT start with input paths that are prefixed with /pnfs/*
# Brandon White, 2022

PATHFILE=$1

while IFS= read -r line
do
	fileurl=https://fndca.fnal.gov:3880/api/v1/namespace/pnfs/fnal.gov/usr/${line}?locality=true
	echo $fileurl
	echo $(curl -k -s ${fileurl}) >> ${PATHFILE}.out
done < ${PATHFILE}
