#!/bin/bash

cp /tmp/x509up_u$(id -u) ./x509up_u1000

podman run \
    --mount 'type=bind,src=./x509up_u1000,dst=/tmp/x509up_u1000' \
    --mount 'type=bind,src=./rucio.cfg,dst=/opt/rucio/etc/rucio.cfg' \
    --mount 'type=bind,src=./datadir,dst=/data' \
    --env 'X509_USER_PROXY=/tmp/x509up_u1000' \
    --env 'PS1=\e[40;32mVIPER@$CONTAINERHOST :\w$ \e[32m' \
    --env "CONTAINERHOST=$HOSTNAME" \
    --name ddm-container \
    -it \
    --rm \
    localhost/ddm-container \
    /bin/bash
