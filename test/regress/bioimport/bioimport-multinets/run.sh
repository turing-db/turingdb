#!/bin/bash

bioimport \
    -json-neo4j /net/db/reactome/json/ \
    -gml /net/gml/melanoma/LR_graph_visu_CAF_Macrophages.gml \
    -net macrophages \
    -gml /net/gml/melanoma/LR_graph_visu_CAF_Treg.gml \
    -net treg \
    -o bioimport-multinets.out \
