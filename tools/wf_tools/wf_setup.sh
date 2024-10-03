#! /bin/bash

PROJECT=$1
DATASET=$2
S3=$3
NF=$4
MODULES=$5
OTP=$6
wd=$(dirname "$0")

mkdir -p "${OTP}/${PROJECT}/${DATASET}/data"
mkdir -p "${OTP}/${PROJECT}/${DATASET}/metadata"
mkdir -p "${OTP}/${PROJECT}/${DATASET}/config"
cp "${wd}/wfconfig/template.config" "${OTP}/${PROJECT}/${DATASET}/config/${DATASET}.gigaConfig"

if [[ "${NF}" != "null" ]]; then
    echo "Checking NF pipeline requirements.."
    mkdir -p "${OTP}/${PROJECT}/${DATASET}/analysis/.nfpipelines/"

    IFS=',' read -r -a NF_list <<<"${NF}"
    for nf_pipeline in "${NF_list[@]}"; do
        if [[ $(ast exist "${nf_pipeline}") == "True" ]]; then
            echo -e "Fetching requirements file for ${nf_pipeline} pipeline.."
            ast s3 cp "s3://turing/nfcore_requirements/.${nf_pipeline}.json" "${OTP}/${PROJECT}/${DATASET}/analysis/.nfpipelines/.${nf_pipeline}.json"
        fi
    done
    echo -e "[DONE].\n"
fi

if [[ "${MODULES}" != "null" ]]; then
    echo "Checking Turing modules requirements.."
    mkdir -p "${OTP}/${PROJECT}/${DATASET}/analysis/notebooks"

    IFS=',' read -r -a MODULE_list <<<${MODULES}
    cat /dev/null >"${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt" # reset module if it existes already
    for module in "${MODULE_list[@]}"; do
        echo "${module}" >>"${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt"
        echo -e "Copying notebooks for module: ${module}.."
        # to substitute with ${TURING_HOME}
        cp -r "/home/dev/turing/src/bioinfo/notebooks/api-notebooks/${module}/baseline_notebooks/" "${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}"
        mkdir -p "${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/results"
    done
    echo -e "[DONE].\n"
fi

ast sync "${OTP}/${PROJECT}/${DATASET}/" "${S3}/${PROJECT}/${DATASET}/"
echo -e "[The end].\n"
