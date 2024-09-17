if [ ${nfcore_run} = "true" ]; then

    # adjust paths in nfcore.yaml
    
    cp ${pod_basedir}/${workflow_name}/${dataset}/${nfcore_name}_params.yaml ${pod_basedir}/${workflow_name}/${dataset}/${nfcore_name}_params_pod.yaml
    jq -c ".params_file[]" ${pod_basedir}/${workflow_name}/${dataset}/analysis/processing/.nfpipelines/${nfcore_name}.json  | 
        while read -r param_columns_pair; do 
            param=$( echo "${param_columns_pair}" | jq -e ".[0]" | sed 's/"//g')
            columns=$( echo "${param_columns_pair}" | jq -e ".[1]" | sed 's/"//g')

            # EDIT PARAMS_FILE PATHS
            awk -v pre=${pod_basedir} -v param=${param} ' { if($1==param ":") {$2= pre "/" $2}; print }' ${pod_basedir}/${workflow_name}/${dataset}/${nfcore_name}_params_pod.yaml > ${pod_basedir}/${workflow_name}/${dataset}/${nfcore_name}_params_pod_tmp.yaml \
            && mv ${pod_basedir}/${workflow_name}/${dataset}/${nfcore_name}_params_pod_tmp.yaml ${pod_basedir}/${workflow_name}/${dataset}/${nfcore_name}_params_pod.yaml

            # EDIT PATHS OF PARAMS FILES
            file_toedit=$( awk -v param=${param} ' { if($1==param ":") print $2}' ${pod_basedir}/${workflow_name}/${dataset}/${nfcore_name}_params_pod.yaml )
            IFS="," read -r -a column_paths <<< ${columns}

            head -1 ${file_toedit} > ${file_toedit}_tmp

            tail -n +2 ${file_toedit} | while IFS=$',' read -r -a line; do
                for col in "${column_paths[@]}"; do
                    line[$((col-1))]="${pod_basedir}/${line[$((col-1))]}"
                done

                echo "${line[*]}" | sed 's/ /,/g' >> ${file_toedit}_tmp
            done
            mv ${file_toedit}_tmp ${file_toedit}

        done