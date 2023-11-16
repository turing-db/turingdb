#!/bin/bash

bioimport \
    -json-neo4j /net/db/reactome/json/ \
    -net reactome \
    -db-path reactome `#Export the db into ./reacome` \
    -o reactome.out

bioimport \
    -gml /net/gml/melanoma/LR_graph_visu_CAF_Macrophages.gml \
    -net macrophages \
    -db reactome `#Update the existing db at ./reactome` \
    -db-path reactome2 `#Export into a new db ./reactome2` \
    -o bioimport-update-existing.out \

