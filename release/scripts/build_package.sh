#!/bin/bash

set -e

build_dir=$HOME/turing/release/build
package_dir=$HOME/turing/release/build/turing_package

rm -rf $build_dir
mkdir -p $build_dir

cd $build_dir && TURING_HOME=$package_dir make -f ../../Makefile -j 8

