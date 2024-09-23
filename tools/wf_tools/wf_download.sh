#!/bin/bash
set -eou pipefail

PROJECT=$1
DATASET=$2
S3=$3
OTP=$4

# Sync project to bucket
ast sync ${S3}/${PROJECT}/${DATASET} ${OTP}/${PROJECT}/${DATASET}
echo -e "\n"

if [[ -f ${OTP}/${PROJECT}/${DATASET}/data/.getdata.txt ]]; then 
    echo -e "Fetching data from:"
    for path in `cat ${OTP}/${PROJECT}/${DATASET}/data/.getdata.txt`; do
        echo -e "$path"
        if [[ $( ast typecheck ${path} | awk '{print $4}' ) == "file" ]]; then
            fl_name=$(basename ${path})
            ast syncup ${path} ${OTP}/${PROJECT}/${DATASET}/data/01.Data/${fl_name}
        elif [[ $( ast typecheck ${path} | awk '{print $4}' ) == "folder" ]]; then
            ast syncup ${path} ${OTP}/${PROJECT}/${DATASET}/data/01.Data/
        else
            echo -e "[S3Uri error]: check provided path in .getdata.txt file."
            exit 1
        fi
    done
fi

if [[ -f ${OTP}/${PROJECT}/${DATASET}/metadata/.getmetadata.txt ]]; then 
    echo -e "Fetching data from:"
    for path in `cat ${OTP}/${PROJECT}/${DATASET}/metadata/.getmetadata.txt`; do
        echo -e "$path"
        if [[ $( ast typecheck ${path} | awk '{print $3}' ) == "file" ]]; then
            fl_name=$(basename ${path})
            ast syncup ${path} ${OTP}/${PROJECT}/${DATASET}/data/01.Data/${fl_name}
        elif [[ $( ast typecheck ${path} | awk '{print $3}' ) == "folder" ]]; then
            ast syncup ${path} ${OTP}/${PROJECT}/${DATASET}/data/01.Data/
        else
            echo -e "[S3Uri error]: check provided path in .getdata.txt file."
            exit 1
        fi
    done
fi


# copy requirements from bucket if needed
requirement_dir="${OTP}/${PROJECT}/requirements"
if [[ -f "${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt" ]]; then
    for module in `cat ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt`; do 
        
        if [[ -f ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json ]]; then 
            # read file (json maybe better), copy data locally, edit path and change it into default parameters
            echo  -e "Requirement file found! Checking required data."
            echo  -e "Data will be fetched from s3 bucket if not already present on local machine."

            for part in $(jq -r "keys[]" ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json); do

                file_path=$( jq -r --arg part ${part}  '.[$part] ' ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json )
                file_name=$( basename ${file_path})
                
                if [[ $(ast exist ${file_path}) == "True" ]]; then
                    echo -e "Checking: ${file_name}"
                    ast syncup ${file_path} ${requirement_dir}/${file_name}
                else 
                    echo -e "[ERROR] file ${file_path} not found in s3 bucekt"
                    exit 1
                fi
            done
            echo -e "[DONE]. All required files have been checked and fetched.\n"

        else
            echo -e "No requirements.json file found. All data are assumed to be already available. \n[DONE].\n"
        fi

    done
fi