#!/bin/bash -ex
# The -e option would make our script exit with an error if any command
# fails while the -x option makes verbosely it output what it does
sudo dnf -y update

sudo dnf install -y gcc gcc-c++ make cmake

mkdir /mnt/pmem0
truncate -s 1G /mnt/pmem0/data


 