#!/bin/bash

sudo service ssh start
/turing/run_jupyter.sh
tail -f /dev/null
