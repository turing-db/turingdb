#!/bin/bash

ssh -i turing_key -L 8088:localhost:8088 dev@172.17.0.2
