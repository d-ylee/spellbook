#!/bin/bash
tarlist_file=$1
MAX_RUNNING=24
MAX_TARSIZE=1000000
running=0
tarsize=0
tarball=0
while read filesize path < ${tarlist_file}; do
    echo $path >> tarball-${tarball}.list
    (( tarsize += filesize ))
    if [ $tarsize -gt $MAX_TARSIZE ]; then
      [ $running -ge $MAX_RUNNING ] && wait -n && (( running-- ))
      (( running++ ))
      tar -cvf tarball-${tarball}.tar --files-from=tarball-${tarball}.list && \
        rm tarball-${tarball}.list &
      (( tarball++ ))
      tarsize=0
    fi
done < $tarlist_file
