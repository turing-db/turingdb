#!/bin/bash

bioimport \
    -neo4j /net/db/source/reactome.graphdb.dump \
    -net reactome \
    -gml /net/gml/melanoma/LR_graph_visu_CAF_Treg.gml \
    -net treg \
    -o bioimport-neo4j.out \
