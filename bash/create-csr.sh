#!/bin/sh
host=$1
dir=`dirname $0`
[ -f $dir/$host ] || mkdir $dir/$host
openssl req -new -newkey rsa:2048 -nodes -keyout $dir/$host/hostkey.pem -subj "/CN=$host.slac.stanford.edu" |\
  tee $dir/$host/hostcert.csr
chmod 600 $dir/$host/hostkey.pem
