#!/bin/bash

mkdir -p $HOME/callgrind
rm $HOME/callgrind/callgrind.out.*

function collect_files() {
    pid=$(ls -t callgrind.out* | xargs | awk '{ print $1}' | cut -d'.' -f3)

    i=0
    for file in $(ls callgrind.out.$pid*)
    do
       cp $file  $HOME/callgrind/callgrind.out.$i
       i=$(($i+1))
    done
}

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
    collect_files
}

valgrind --tool=callgrind --instr-atstart=no $@


