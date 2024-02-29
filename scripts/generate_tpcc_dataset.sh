#!/bin/bash

make clean -C demo/demo-tpc-c/tpcc-generator/
make -C demo/demo-tpc-c/tpcc-generator/

rm -rf demo/demo-tpc-c/data
mkdir demo/demo-tpc-c/data

./demo/demo-tpc-c/tpcc-generator/tpcc-generator $1 demo/demo-tpc-c/data