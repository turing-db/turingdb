#!/usr/bin/bash

ln -s $ACTIVE_REPO/src/samples/prefix-tree/sample.sh $ACTIVE_REPO/build/build_package/samples/prefix-tree/sample.sh
chmod +x !!:3

# Get the linux words file into build
wget -P $ACTIVE_REPO/build/build_package/samples/prefix-tree/data/ http://www.cs.duke.edu/~ola/ap/linuxwords
