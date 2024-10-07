#!/bin/bash
set -eou pipefail

gigaConfig=$1
copymode=$2
suffix=$3

dataset=$(jq -r '.submission.dataset' "${gigaConfig}")
basedir=$(jq -r '.submission.basedir' "${gigaConfig}") && basedir=${basedir%/}
s3bucket=$(jq -r '.submission.s3bucket' "${gigaConfig}") && s3bucket=${s3bucket%/}

if [[ "${copymode}" == "True" ]]; then
    if [[ "${suffix}" != "null" ]]; then
        echo -e "Running: wf save -config ${gigaConfig} -copy -suffix ${suffix}"
        echo -e "Copying local project to s3 bucket folder: ${s3bucket}/${dataset}_${suffix}.."
        if [[ $(ast exist "${s3bucket}/${dataset}_${suffix}") == "True" ]]; then
            echo -e "\n[WARNING]: ${s3bucket}/${dataset}_${suffix} already exists."
            echo -e "           Local project will be sync'ed to already existing s3 bucket folder.\n"
        fi
        ast s3 sync "${basedir}/${dataset}/" "${s3bucket}/${dataset}_${suffix}/" && echo "[DONE]."
    else
        echo -e "Running: wf save -config ${gigaConfig} -copy"
        check=True
        COUNTER=0
        while [[ "${check}" == True ]]; do
            COUNTER=$(("${COUNTER}" + 1))
            check=$(ast exist "${s3bucket}/${dataset}_${COUNTER}")
            #echo ${COUNTER}
        done
        echo -e "We are going to save a copy of the local project into ${dataset}_${COUNTER} in the user's s3 bucket."
        ast s3 sync "${basedir}/${dataset}/" "${s3bucket}/${dataset}_${COUNTER}/" && echo "[DONE]."
    fi
else
    echo -e "Running: wf save -config ${gigaConfig}"
    echo -e "Overwriting project into s3 bucket.."
    echo -e "All files except those in data/ will be cleared."
    ast rm "${s3bucket}/${dataset}/" --exclude "data/*" --recursive --quiet
    ast sync "${basedir}/${dataset}/" "${s3bucket}/${dataset}/" && echo "[DONE]."
fi
