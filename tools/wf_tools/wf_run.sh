#!/bin/bash

gigaConfig=$1
wd=$(dirname $0)

basedir=$(jq -r '.submission.basedir' "${gigaConfig}") && basedir=${basedir%/}
project_name=$(jq -r '.submission.project_name' "${gigaConfig}")
dataset=$(jq -r '.submission.dataset' "${gigaConfig}")

source ${basedir}/${project_name}/${dataset}/config/${project_name}_${dataset}_workflow.config
basedir=${basedir%/}
s3bucket=${s3bucket%/}

#######################
## NEXTFLOW ROUTINES ##
if [ ${libraryprep_run} = "true" ] || [ ${nfcore_run} = "true" ] ; then
    echo -e "Sync'ing up the project in the s3 bucket"
    ast syncup ${basedir}/${project_name}/${dataset}/ ${s3bucket}/${project_name}/${dataset}/
    echo -e "[DONE].\n"

    echo -e "Nextflow flags are set to true. Submitter pod is engaged.\n"
    
    #source ${basedir}/${project_name}/${dataset}/config/${dataset}_libraryprep_params.config
#   source nfcore config - to do 
    nspace=$(jq -r '.nfcore_k8s_config.k8s.namespace' ${gigaConfig} )
    pod_basedir=$(jq -r '.nfcore_k8s_config.k8s.storageMountPath' ${gigaConfig})

    # removing failed pods in the system
    echo -e "Wiping failed pods in memory in namaspace"
    kubectl delete pod --field-selector=status.phase="Failed" -n ${nspace}
    echo -e "[DONE].\n"

    kubectl exec ${submitter_pod} -n ${nspace} -- /bin/sh -c "./scratch/wf_run_nf.sh ${s3bucket} ${project_name} ${dataset} ${pod_basedir}"

fi

#######################
## BIORUN ROUTINES ##
if [ ${biorun} = "true" ]; then
    final_exitcode=0

    echo -e "Biorun flag is set to true. Running biorun routine.."
    for module in `cat ${basedir}/${project_name}/${dataset}/analysis/notebooks/.modules.txt`; do
        echo -e "Turing module: ${module}"
        if [[ ${subset_specfic_params} == "true" ]] ; then
            # Creating subset specific
            mkdir -p ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/tmp

            # if tmp is empty, make_nb flag to True -> make subset specific notebooks
            make_nb="false"
            if [ $(ls -A "${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/tmp" | wc -l) -eq 0 ]; then      
                mv ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/*.ipynb ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/tmp
                make_nb="true"
            fi
                
            for subset in $(jq -r '.submission.subset_list | .[]' ${gigaConfig} ); do
                if [[ ${make_nb} == "true" ]]; then # this block is to make sure that you can rerun biorun smoothly a second time, without having to reset the baseline nb as before
                    for nb in `ls ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/tmp/*.ipynb`; do
                        bsname=$(basename ${nb} .ipynb)
                        cp ${nb} ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/${bsname}_${subset}.ipynb
                    done
                fi

                echo -e "Running analays on ${dataset}, subset: ${subset}.."
                ( biorun -html -o ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/${subset}.out \
                ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/*-api-*${subset}.ipynb \
                -nbarg config=${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/config_${project_name}_${dataset}_${subset}.json && echo -e "[DONE]." ) ||  final_exitcode=1

            done
        else
            echo -e "Running analays on ${dataset}"
            ( biorun -html -o ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/${dataset}.out \
            ${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/*-api-*.ipynb \
            -nbarg config=${basedir}/${project_name}/${dataset}/analysis/notebooks/${module}/config_${project_name}_${dataset}.json && echo -e "[DONE]." ) ||  final_exitcode=1
            
        fi
    done 

    if [[ ${s3sync} == "true" ]]  && [[ ${final_exitcode} -eq 0 ]]; then # not robust to earlier errors than when final_exitcode is actually updated
        ast syncup ${basedir}/${project_name}/${dataset} ${s3bucket}/${project_name}/${dataset}
    fi

fi