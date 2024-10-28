#! /bin/bash
set -e

PROJECT=$1
DATASET=$2
S3=$3
NF=$4
MODULES=$5
OTP=$6
wd=$(dirname "$0")

mkdir -p "${OTP}/${PROJECT}/${DATASET}/data/01.Data"
touch "${OTP}/${PROJECT}/${DATASET}/data/.getdata.txt"
mkdir -p "${OTP}/${PROJECT}/${DATASET}/metadata"
touch "${OTP}/${PROJECT}/${DATASET}/metadata/.getmetadata.txt"
mkdir -p "${OTP}/${PROJECT}/${DATASET}/config"
cp "${wd}/wfconfig/template.config" "${OTP}/${PROJECT}/${DATASET}/config/${DATASET}.gigaConfig_tmp"

# Let's update the gigaConfig with command line parameters
yq \
    --arg PROJECT "${PROJECT}" \
    --arg DATASET "${DATASET}" \
    --arg S3 "${S3}" \
    --arg OTP "${OTP}" \
    '.submission.project_name = $PROJECT | 
    .submission.dataset = $DATASET  |
    .submission.s3bucket = $S3 |
    .submission.basedir = $OTP ' "${OTP}/${PROJECT}/${DATASET}/config/${DATASET}.gigaConfig_tmp" >"${OTP}/${PROJECT}/${DATASET}/config/${DATASET}.gigaConfig"
rm "${OTP}/${PROJECT}/${DATASET}/config/${DATASET}.gigaConfig_tmp"

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

    IFS=',' read -r -a MODULE_list <<<"${MODULES}"
    cat /dev/null >"${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt" # reset module if it existes already
    for module in "${MODULE_list[@]}"; do
        echo "${module}" >>"${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/.modules.txt"
        echo -e "Copying notebooks for module: ${module}.."
        baseline_notebooks="${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}"
        if [[ -d "${baseline_notebooks}" ]]; then
            # empty the folder if it already exists. Make sure to remove also hidden files
            rm -rf "${baseline_notebooks:?}"/* # with :? the command fails if variable undefined -> avoid deleting content of root!!!
        fi
        # ${TURING_HOME%/*/*} gives you /home/dev/turing
        cp -rT "${TURING_HOME%/*/*}/src/bioinfo/notebooks/api-notebooks/${module}/baseline_notebooks/" "${baseline_notebooks}"
        mkdir -p "${OTP}/${PROJECT}/${DATASET}/analysis/notebooks/${module}/results"
    done
    echo -e "[DONE].\n"
fi

ast sync "${OTP}/${PROJECT}/${DATASET}/" "${S3}/${PROJECT}/${DATASET}/"
echo -e "[The end].\n"
