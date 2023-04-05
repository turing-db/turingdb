#!/bin/bash

docker container stop turing
docker container rm turing
docker run -t -d --name turing turing-platform:latest
