#!/bin/bash
rm -Rf /tmp/bjwhite/.rucio_bjwhite
klist -a
kx509
voms-proxy-init -voms dune:/dune/Role=Analysis -noregen
mydate=`date +%Y%m%d`
if [ $# -eq 1 ]
then
   suffix=".${1}"
else
   suffix=""
fi

dd if=/dev/zero of=/tmp/1000kbtestfile.${mydate}${suffix} bs=1024 count=1000
export PATH="/usr/local/sbin:/usr/local/bin:${PATH}"
export PYTHONPATH="/usr/local/lib/python3.6/site-packages:${PATH}"
export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app
metacat auth login -m x509 -c /tmp/x509up_u1000 dunepro
metacat file declare test:1000kbtestfile.${mydate}${suffix} dune:all -s 1024000000 -c 'adler32:93b4001' -m '{"core.data_tier": "test", "dune.campaign": "test", "core.file_type": "test", "core.data_stream": "test", "core.run_type": "test"}'
rucio -v -a test upload --rse DUNE_US_FNAL_DISK_STAGE --lifetime 172000 --scope test --register-after-upload --protocol davs --name 1000kbtestfile.${mydate}${suffix} /tmp/1000kbtestfile.${mydate}${suffix}
myrc=$?
if [ $myrc -ne 0 ]
then echo "rucio upload not successful, exiting.  You may have to remove test file manually from pnfs"
        exit $myrc
fi
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 DUNE_CERN_EOS
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 DUNE_ES_PIC
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 DUNE_FR_CCIN2P3_DISK
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 DUNE_IN_TIFR
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 DUNE_US_BNL_SDCC
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 LANCASTER
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 MANCHESTER
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 NIKHEF
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 PRAGUE
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 QMUL
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 RAL_ECHO
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 RAL-PP
rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 SURFSARA
#rucio add-rule --account=test --lifetime 172800 test:1000kbtestfile.${mydate}${suffix} 1 T3_US_NERSC
rucio list-rules --account=test | grep 1000kbtestfile.${mydate}${suffix}
