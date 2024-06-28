#!/bin/bash

bioimport -gml do_not_exist.gml
result=$?

if [ $result -ne 0 ] ; then
    exit 0
else
    exit 1
fi
