#!/bin/bash
set -eou pipefail

source_json=$1 

# Let's read in the destination dir from the input json and create a variable
basedir=$(jq -r '.submission.basedir' "${source_json}")
basedir=${basedir%/}
project_name=$(jq -r '.submission.project_name' "${source_json}")
dataset=$(jq -r '.submission.dataset' "${source_json}")
#mkdir -p ${basedir}

## export bash config 
paste <(jq -r ".submission | to_entries | .[] |(.key) " "${source_json}")  \
      <(jq -rc ".submission | to_entries | .[] |(.value) " "${source_json}") | 
      awk '{print $1 "="  "\"" $2  "\""}' > ${basedir}/${project_name}/${dataset}/config/${project_name}_${dataset}_workflow.config 

source ${basedir}/${project_name}/${dataset}/config/${project_name}_${dataset}_workflow.config
echo -e "Bash config created: ${basedir}/${project_name}/${dataset}/config/${project_name}_${dataset}_workflow.config"
####

## export library prep params.yaml, if this step is true
if [[ "$libraryprep_run" == "true" ]]; then
    ## Export yaml files
    #jq ".libraryprep_nf" "$source_json" | yq --yaml-output > ${basedir}/${dataset}/${dataset}_libraryprep_params.yaml
    
    paste <(jq -r ".libraryprep_nf | to_entries | .[] |(.key) " "${source_json}")  \
          <(jq -rc ".libraryprep_nf | to_entries | .[] |(.value) " "${source_json}") | 
        awk '{print $1 "="  "\"" $2  "\""}' > ${basedir}/${project_name}/${dataset}/config/${dataset}_libraryprep_params.config


    echo -e "NF params file for the libraryprep workflow created: ${basedir}/${project_name}/${dataset}/config/${dataset}_libraryprep_params.yaml"
fi

## export nfcore pipeline specific parmas.yaml
if [[ "$nfcore_run" == "true" ]]; then
    jq ".nf_core" "$source_json" | yq --yaml-output > ${basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params.yaml
    echo -e "NF params file for the ${nfcore_name} workflow created: ${basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params.yaml"

    # export nfcore nextflow.cofig
    for part in $(jq -r ".nfcore_k8s_config |   keys[] " "$source_json"); do 
        echo ${part} "{" 
        paste <( jq -r --arg part "$part" '.nfcore_k8s_config.[$part] | to_entries | .[] | (.key)' "$source_json" ) \
        <( jq --arg part "$part" '.nfcore_k8s_config.[$part] | to_entries | .[] | (.value)' "$source_json" ) | 
        awk  'BEGIN{OFS=""};{$1=$1 "="; print}'
        echo "}" 
    done |  
    sed 's/\"\[/\[/; s/\]\"/\]/' > ${basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_k8s.config
    echo -e "K8s config file for the ${nfcore_name} workflow created: ${basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_k8s.config"
fi
####
