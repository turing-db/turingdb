#!/bin/bash

set -e

build_dir=/home/dev/turing/release/build
package_dir=/home/dev/turing/release/build/turing_package

source /turing/setup.sh
printenv

rm -rf $build_dir
mkdir -p $build_dir

cd $build_dir && TURING_HOME=$package_dir make -f ../../Makefile -j 8

