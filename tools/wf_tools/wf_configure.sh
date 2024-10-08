#!/bin/bash

gigaConfig=$1
wd=$(dirname "$0")

basedir=$(jq -r '.submission.basedir' "${gigaConfig}") && basedir=${basedir%/}
project_name=$(jq -r '.submission.project_name' "${gigaConfig}")
dataset=$(jq -r '.submission.dataset' "${gigaConfig}")
requirement_dir="${basedir}/${project_name}/requirements"

bash "${wd}"/wfconfig/split_config.sh "${gigaConfig}"
source "${basedir}/${project_name}/${dataset}/config/${project_name}_${dataset}_workflow.config"
basedir=${basedir%/}

# NEXTFLOW
if [[ "${nfcore_run}" == "true" ]]; then
    mkdir -p "${basedir}/${project_name}/${dataset}/analysis/${dataset}_${nfcore_name}_processing/Inputs"
fi

# BIORUN
if [[ "${biorun}" == "true" ]]; then
    for module in $(cat "${basedir}/${project_name}/${dataset}/analysis/notebooks/.modules.txt"); do
        echo -e "Creating parameter files for module: ${module}."
        if [[ "${subset_specfic_params}" == "true" ]]; then
            # Creating subset specific
            echo -e "Creating subset specific config files:"
            for subs in $(jq -r '.submission.subset_list | .[]' "${gigaConfig}"); do
                echo -e "---> ${subs}: ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/config_${project_name}_${dataset}_${subs}.json"
                tmp=$(jq --arg subs "${subs}" --arg requirement_dir "${requirement_dir}" '{global: .submission} | .global.subset = $subs | .global.requirement_dir = $requirement_dir ' "${gigaConfig}")
                jq -s '.[0] * .[1]' \
                    <(echo "${tmp}") \
                    <(cat "${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/default_parameters.json") \
                    >"${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/config_${project_name}_${dataset}_${subs}.json" && echo -e "\t[DONE]."
            done
        else
            echo -e "--> ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/config_${project_name}_${dataset}.json"
            tmp=$(jq --arg requirement_dir "${requirement_dir}" '{global: .submission} | .global.requirement_dir = $requirement_dir ' "${gigaConfig}")
            jq -s '.[0] * .[1]' \
                <(echo "${tmp}") \
                <(cat "${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/default_parameters.json") \
                >"${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/config_${project_name}_${dataset}.json" && echo -e "\t[DONE].\n"
        fi
    done
fi

# SYNC UP
if [[ "${s3sync}" == "true" ]]; then
    echo -e "\nNow sync'ing up project folder"
    s3bucket=${s3bucket%/}
    #aws s3 rm ${s3bucket}/${dataset}/ --recursive --quiet
    ast syncup "${basedir}/${project_name}/${dataset}" "${s3bucket}/${project_name}/${dataset}" && echo "[DONE]."
fi
