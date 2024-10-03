#! /bin/bash

s3bucket=$1
project_name=$2
dataset=$3
pod_basedir=$4
####################

# sync project locally
echo -e "Fetching project from S3 bucket into submitter pod at: ${pod_basedir}/${project_name}/${dataset}.."
ast syncup "${s3bucket}/${project_name}/${dataset}" "${pod_basedir}/${project_name}/${dataset}"
echo -e "[DONE].\n"

source "${pod_basedir}/${project_name}/${dataset}/config/${project_name}_${dataset}_workflow.config"

if [[ "${libraryprep_run}" = "true" ]]; then
    echo -e "Nextflow: libraryprep flag set to true. Cleaning up sequencing libraries.."

    source "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_libraryprep_params.config"
    ## Additional arguments # EDIT! check if they are not null!!
    #samplesheet_name=$5 #$( realpath $2 )
    #samplesheet_columns=$6 # comma separated columns of samplesheet. ex: 4,5
    IFS="," read -r -a column_paths <<<"${samplesheet_columns}"

    ## Edit samplesheets
    echo -e "    Editing data paths in samplesheet. Project basedir: ${pod_basedir}"
    samplesheet="${pod_basedir}/${project_name}/${dataset}/data/${samplesheet_name}"
    samp_basename="${samplesheet%.*}"
    ext="${samplesheet##*.}"
    pod_samplesheet="${samp_basename}_pod.${ext}"

    head -1 $samplesheet >"$pod_samplesheet"

    tail -n +2 $samplesheet | while IFS=$',' read -r -a line; do
        for col in "${column_paths[@]}"; do
            line["$((col - 1))"]="${pod_basedir}/${line[$((col - 1))]}"
        done

        echo "${line[*]}" | sed 's/ /,/g' >>"$pod_samplesheet"
    done
    echo -e "    [DONE].\n"

    ## Run nextflow - library prep
    echo -e "    Now running nf-turing/libraryprep"
    echo -e "    results will be stored in: ${pod_basedir}/${project_name}/${dataset}/data/02.CleanData"
    export NXF_WORK="${pod_basedir}/${project_name}/${dataset}_work"
    NXF_LAUNCH="${pod_basedir}/${project_name}/${dataset}_launch"
    mkdir -p "${NXF_LAUNCH}"
    cd "${NXF_LAUNCH}"

    nextflow run $"{pod_basedir}/nextflow/nf-turing/libraryprep" \
        -profile docker \
        --samplesheet "${pod_samplesheet}" \
        --outdir "${pod_basedir}/${project_name}/${dataset}/data/02.CleanData"
    echo "    [DONE]."

    mv ${pod_basedir}/${project_name}/${dataset}/data/02.CleanData/*r2r.csv ${pod_basedir}/${project_name}/${dataset}/data
    ast syncup ${pod_basedir}/${project_name}/${dataset} ${s3bucket}/${dataset}
fi

if [[ ${nfcore_run} = "true" ]]; then
    # EDIT PATHS
    echo -e "Correcting the input file paths within the submitter pod.."
    cp "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params.yaml" "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params_pod.yaml"
    jq -c ".params_file[]" "${pod_basedir}/${project_name}/${dataset}/analysis/.nfpipelines/.${nfcore_name}.json" |
        while read -r param_columns_pair; do
            param=$(echo "${param_columns_pair}" | jq -e ".[0]" | sed 's/"//g')
            columns=$(echo "${param_columns_pair}" | jq -e ".[1]" | sed 's/"//g')

            # EDIT PARAMS_FILE PATHS
            prepend_var="${pod_basedir}/${project_name}/${dataset}/analysis/${dataset}_${nfcore_name}_processing/Inputs"
            # Here we read the params file. we accept both a blank space or no space between key and value
            awk -v pre="${prepend_var}" -v param="${param}" \
                ' { if ($1~param ":") { 
                    if (NF==1) { split($1,a,":"); $1= a[1] ":" pre "/" a[2] } 
                    else {$2= pre "/" $2} }
                    print $0 
                } ' "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params_pod.yaml" >"${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params_pod_tmp.yaml" &&
                mv "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params_pod_tmp.yaml" "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params_pod.yaml"

            # EDIT PATHS OF PARAMS FILES # there always has to be a header..
            file_toedit=$(awk -v param="${param}" \
                ' { if ($1~param ":") { 
                    if (NF==1) { split($1,a,":"); print a[2] } 
                    else {print $2} }
                } ' "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params_pod.yaml")
            IFS="," read -r -a column_paths <<<"${columns}"

            head -1 "${file_toedit}" >"${file_toedit}_tmp"

            tail -n +2 "${file_toedit}" | while IFS=$',' read -r -a line; do
                for col in "${column_paths[@]}"; do
                    line[$((col - 1))]="${pod_basedir}/${line[$((col - 1))]}"
                done

                echo "${line[*]}" | sed 's/ /,/g' >>"${file_toedit}_tmp"
            done
            mv "${file_toedit}_tmp" "${file_toedit}"

        done
    echo -e "[DONE].\n"

    # Fetch nfcore requirements..
    if [ -f "${pod_basedir}/${project_name}/${dataset}/analysis/.nfpipelines/.${nfcore_name}.json" ]; then # if file exits
        echo -e "Fetching nf-core pipeline requirements.."
        # check if there are any requirements (list not empty!!)
        # AND CHECK IF NOT ALREADY DOWNLOADED
        for path in $(jq -c ".requirements[]" "${pod_basedir}/${project_name}/${dataset}/analysis/.nfpipelines/.${nfcore_name}.json" | sed 's/"//g'); do
            bname=$(basename "${path}")
            echo -e "Fetching: ${path}"
            ast syncup "${path}" "${pod_basedir}/${project_name}/${dataset}/analysis/.nfpipelines/${bname}"
        done
        echo -e "[DONE].\n"
    fi

    # Run nextflow - nfcore
    echo -e "Now running nextflow.."
    # workDir
    export NXF_WORK="${pod_basedir}/${project_name}/${dataset}_nfcore_work"
    # outDir # maybe need to first mkdir?
    export NXF_OUT="${pod_basedir}/${project_name}/${dataset}/results/${dataset}_processing"
    # launchDir # there is no official nf env variable for the launch dir..
    NXF_LAUNCH="${pod_basedir}/${project_name}/${dataset}_nfcore_launch"
    mkdir -p "${NXF_LAUNCH}"
    cd "${NXF_LAUNCH}"

    nextflow run "${pod_basedir}/nextflow/nf-core/${nfcore_name}" \
        -profile docker \
        -params-file "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_params_pod.yaml" \
        -c "${pod_basedir}/${project_name}/${dataset}/config/${dataset}_${nfcore_name}_k8s.config" \
        --outdir "${NXF_OUT}" \
        -resume

    # Let's catch the error status of the current run from the logs and optionally sync to the s3 bucket
    latest_nfrun=$(nextflow log | sed 's/ //g' | awk -F "\t" 'END {print $3}')
    err_status=$(nextflow log | sed 's/ //g' | awk -F "\t" 'END {print $4}')
    echo "${err_status}" >"${NXF_LAUNCH}/${latest_nfrun}_STATUS.txt"

    if [[ ${err_status} == "OK" ]]; then
        ast sync "${NXF_OUT}/" "${s3bucket}/${project_name}/${dataset}/analysis/${dataset}_${nfcore_name}_processing/results/"
    elif [[ ${err_status} == "ERR" ]]; then
        echo -e "[ERR]: pipeline run ${latest_nfrun} completed with errors."
        dt=$(date +"%Y_%m_%d_%H_%M_%S")
        mkdir -p "${pod_basedir}/${project_name}/${dataset}/.pods"
        kubectl get events -n "${nspace}" >"${pod_basedir}/${project_name}/${dataset}/.pods/pod_events_${dt}.txt"
        exit 1
    fi

fi

echo "[DONE]."
