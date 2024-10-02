#!/bin/bash

# Check if s3:// prefix is present
if [[ "$1" != s3://* ]]; then
    s3uri="s3://$1"
else
    s3uri="$1"
fi

# Remove trailing slash for consistent path handling
s3uri="${s3uri%/}"

output=$(aws s3 ls "$s3uri" 2>/dev/null)

if [ -z "$output" ]; then
    echo "[ERROR]: S3Uri does not exist"
else
    basename_s3uri=$(basename "$s3uri")
    if echo "$output" | grep -q "^ *PRE *${basename_s3uri}\/$"; then
        echo "${s3uri} is a folder"
    elif echo "$output" | grep -q "\s\S* ${basename_s3uri}$"; then
        echo "${s3uri} is a file"
    else
        echo -e "[ERROR]: ambiguous paths provided"
        exit 1
    fi
fi
