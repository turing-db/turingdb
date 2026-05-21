#!/bin/bash
./ir "$@" | awk '/^flowchart/,0' | curl -s https://mermaid-ascii.art --data-urlencode 'mermaid@-'
echo ""
