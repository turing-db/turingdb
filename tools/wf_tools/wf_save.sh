#!/bin/bash

if [[ -f ~/.bash_aliases ]]; then
    . ~/.bash_aliases
fi

gigaConfig=$1
copymode=$2
wd=$(dirname $0)

dataset=$(jq -r '.submission.dataset' "${gigaConfig}")
basedir=$(jq -r '.submission.basedir' "${gigaConfig}") && basedir=${basedir%/}
s3bucket=$(jq -r '.submission.s3bucket' "${gigaConfig}") && s3bucket=${s3bucket%/}

if [[ "${copymode}" = True ]]; then
    echo -e "Running: wf save -config ${gigaConfig} -copy"
    check=True
    COUNTER=0
    while [[ "${check}" == True ]]; do
        COUNTER=$(expr $COUNTER + 1)
        check=$(ast exist ${s3bucket}/${dataset}_${COUNTER})
        #echo ${COUNTER}
    done
    echo -e "We are going to save a copy of the project called ${dataset}_${COUNTER} into the s3 bucket."
    aws s3 sync ${basedir}/${dataset} ${s3bucket}/${dataset}_${COUNTER}/ && echo "[DONE]."

else
    echo -e "Running: wf save -config ${gigaConfig}"
    echo -e "Overwriting project into s3 bucket"
    aws s3 rm ${s3bucket}/${dataset}/ --recursive --quiet
    aws s3 sync ${basedir}/${dataset} ${s3bucket}/${dataset}/ && echo "[DONE]."
fi