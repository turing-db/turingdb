#!/bin/bash

source_path=$1
destination_path=$2
wd=$(dirname $0)
###############

# Source can be either local path or an s3uri. check which type it is.
# Then, if source is a file, we use cp. if a folder, we use sync

# If source is s3uri
if [[ $source_path ==  s3://* ]]; then
    # Check if path exists
    if [ "$(bash ${wd}/ast_exist.sh "$source_path")" == "True" ]; then
        otp=$( aws s3 ls "$source_path")
        # Determine if the S3 path is a file or directory
        if echo ${otp} | grep -q -E "^ *PRE "; then
            # it's a folder
            aws s3 sync "$source_path/" "$destination_path" # "/" here is important! if you remove it breaks
        else
            # it's a file
            # if destination file already exists locally, copy only if:
            # - timestamp is older
            # OR
            # - if timestamps are equal, but file size differs (= file has changed)
            if [ -f "${destination_path}" ]; then
                timestamp_destination=$(stat -c %Y "${destination_path}")
                size_destination=$(stat -c %s "${destination_path}")

                timestamp_source=$( aws s3 ls ${source_path} | awk '{print $1,$2}')
                # convert timestamp in epoch time
                timestamp_source=$(date -d "${timestamp_source}" +%s)
                size_source=$( aws s3 ls ${source_path} | awk '{print $3}' )

                # if timestamp of source file is more recent (i.e. greater epoch time), then copy
                if [[ "${timestamp_destination}" -lt "${timestamp_source}" ]]; then
                    #but first let's check that there is not less than 10s difference, otherwise it's unreliable
                    diff=$((timestamp_source - timestamp_destination))
                    if [ ${diff} -lt 10 ]; then
                        echo -e "[Warning]: timestamp difference is less than 10 seconds. Are you sure that destination file is outdated?"
                    fi
                    aws s3 cp "$source_path" "$destination_path"
                # if timestamp is the same, but size differs, then copy
                elif [[ "${timestamp_destination}" -eq "${timestamp_source}" ]] && [[ "${size_destination}" != "${size_source}" ]]; then
                    aws s3 cp "$source_path" "$destination_path"
                fi
            else
                aws s3 cp "$source_path" "$destination_path"
            fi
        fi
    else
        echo "Error: s3 object does not exist."
    fi
else # source is local object
    if [ -d "$source_path" ]; then
        aws s3 sync "$source_path" "$destination_path"
    elif [ -f "$source_path" ]; then
        # if destination file already exists in s3 bucket
        if [ "$(bash ${wd}/ast_exist.sh "${destination_path}")" == "True"  ]; then
            timestamp_destination=$(aws s3 ls "${destination_path}" | awk '{print $1,$2}')
            # convert timestamp in epoch time
            timestamp_destination=$(date -d "${timestamp_destination}" +%s)
            size_destination=$(aws s3 ls "${destination_path}" | awk '{print $3}')

            timestamp_source=$( stat -c %Y  "${source_path}" )
            size_source=$( stat -c %s "${source_path}" )
            
            # if source file timestamp is more recent (i.e. greater epoch time), then copy
            if [[ "${timestamp_destination}" -lt "${timestamp_source}" ]]; then
                diff=$((timestamp_source - timestamp_destination))
                if [ ${diff} -lt 10 ]; then
                    echo -e "[Warning]: timestamp difference is less than 10 seconds. Are you sure that destination file is outdated?"
                fi
                aws s3 cp "$source_path" "$destination_path"
            # if timestamp is the same, but size differs, then copy
            elif [[ "${timestamp_destination}" -eq "${timestamp_source}" ]] && [[ "${size_destination}" != "${size_source}" ]]; then
                    aws s3 cp "$source_path" "$destination_path"
            fi
        else
            aws s3 cp "$source_path" "$destination_path"
        fi
        
    else
        echo "Error: local object does not exist."
    fi
fi