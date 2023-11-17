#!/bin/bash

bioimport \
    -json-neo4j /net/db/reactome/json/ \
    -net reactome \
    -gml /net/gml/scott/zip_covid_diag.gml \
    -net scott_db \
    -db-path reactome_scott \
    -o reactome.out

bioimport \
    -gml ./linkage.gml \
    -db ./reactome_scott \
    -db-path ./reactome_scott_linked


