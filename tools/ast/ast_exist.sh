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
    echo "False"
else
    basename_s3uri=$(basename "$s3uri" )
    # Check for exact match
    if echo "$output" | grep -q -E "^ *PRE *${basename_s3uri}\/|\s\S* ${basename_s3uri}$"; then
        echo "True"
    else
        echo "False"
    fi
fi