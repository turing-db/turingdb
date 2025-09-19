#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <total_queries> <input_wordlist> <output_file>"
    echo "  total_queries: Number of queries to generate"
    echo "  input_wordlist: Path to input word list file"
    echo "  output_file: Path to output file"
    echo ""
    echo "Generates queries with:"
    echo "  - 50% real words from wordlist"
    echo "  - 25% truncated words (0-3 chars removed)"
    echo "  - 25% random strings (5-15 chars)"
    exit 1
fi

shuf -n $(($1/2)) $2 > $3

shuf -n $(($1/4)) $2 | while read word; do 
    len=${#word}; 
    trunc=$((RANDOM % 4)); 
    newlen=$((len > trunc ? len - trunc : 1)); 
    echo "${word:0:$newlen}"
done >> $3

for i in $(seq 1 $(($1/4))); do
    len=$((RANDOM % 11 + 5));
    tr -dc 'a-z' < /dev/urandom | head -c $len
    echo
done >> $3
