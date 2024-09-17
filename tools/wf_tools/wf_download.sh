#!/bin/bash

PROJECT=$1
DATASET=$2
S3=$3
OTP=$4

# Sync project to bucket
ast sync ${S3}/${PROJECT}/${DATASET} ${OTP}/${PROJECT}/${DATASET}
echo -e "\n"

## TO ADD: -> check if it is a folder or a file in s3bucket and use either sync or cp. it could also be a local file, not in the bucket
if [[ -f ${OTP}/${PROJECT}/${DATASET}/data/.getdata.txt ]]; then 
    echo -e "Fetching data from:"
    for path in `cat ${OTP}/${PROJECT}/${DATASET}/data/.getdata.txt`; do
        echo -e "$path"
        ast sync ${path} ${OTP}/${PROJECT}/${DATASET}/data/01.Data
    done
fi

if [[ -f ${OTP}/${PROJECT}/${DATASET}/metadata/.getmetadata.txt ]]; then 
    echo -e "Fetching data from:"
    for path in `cat ${OTP}/${PROJECT}/${DATASET}/metadata/.getmetadata.txt`; do
        echo -e "$path"
        ast sync ${path} ${OTP}/${PROJECT}/${DATASET}/metadata/
    done
fi


# copy requirements from bucket if needed
requirement_dir="${OTP}/${PROJECT}/requirements"
if [[ -f "${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt" ]]; then
    for module in `cat ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt`; do 
        
        if [[ -f ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json ]]; then 
            # read file (json maybe better), copy data locally, edit path and change it into default parameters
            echo  -e "Requirement file found! Checking required data.."

            for part in $(jq -r "keys[]" ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json); do
            
                mkdir -p ${requirement_dir} # create a subdirectory with the outer key name in json
                for key in $( jq -r --arg part ${part}  '.[$part] | keys[]' ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json ); do
                    file_path=$( jq -r --arg part ${part} --arg key ${key} '.[$part][$key] [0]' ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json )
                    file_name=$( basename ${file_path})
                    file_type=$( jq -r --arg part ${part} --arg key ${key} '.[$part][$key] [1]' ${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/requirements.json )

                    if [[ ! -e ${requirement_dir}/${file_name} ]]; then # if file does not exist locally, copy it from s3 bucket
                        echo -e "${file_name} not found in ${requirement_dir}. Fetching it from s3 bucket.."
                        if [[ $(ast exist ${file_path}) == "True" ]]; then
                            if [[ ${file_type} == "dir" ]]; then
                                aws s3 cp ${file_path} ${requirement_dir}/${file_name} --recursive && echo -e "[DONE].\n"
                            elif [[ ${file_type} == "file" ]]; then
                                aws s3 cp ${file_path} ${requirement_dir}/${file_name} && echo -e "[DONE].\n"
                            fi
                        else 
                            echo -e "[ERROR] file ${file_path} not found in s3 bucekt"
                            exit 1
                        fi
                    else
                        echo -e "${requirement_dir}/${file_name} found."
                    fi
                done
            done
            echo -e "[DONE]. All required files have been checked.\n"

        else
            echo -e "No requirements.json file found. All data are assumed to be already available. \n[DONE].\n"
        fi

    done
fi