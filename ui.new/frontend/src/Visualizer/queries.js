import React from 'react';
import axios from 'axios';
import { useQuery } from "../App/queries"
import { useSelector } from 'react-redux';

export const useElementsQuery = () => {
    const selectedNodes = useSelector(state => state.selectedNodes);
    const dbName = useSelector(state => state.dbName);
    const hiddenNodeIds = useSelector(state => state.visualizer.hiddenNodeIds);
    const filters = useSelector(state => state.visualizer.filters);
    const nodeIds = Object.keys(selectedNodes);

    return useQuery(
        ["cytoscape_elements", dbName, nodeIds, hiddenNodeIds, filters],
        React.useCallback(async () => {
            const rawElements = await axios
                .post("/api/viewer/init", {
                    db_name: dbName,
                    node_ids: nodeIds,
                    hidden_node_ids: hiddenNodeIds,
                    max_edge_count: 30,
                    node_property_filter_out: [
                        ...(filters.hideCompartments ? [
                            ["schemaClass", "Compartment"]
                        ] : []),

                        ...(filters.hideSpecies ? [
                            ["schemaClass", "Species"]
                        ] : []),

                        ...(filters.hidePublications ? [
                            ["schemaClass", "InstanceEdit"],
                            ["schemaClass", "ReviewStatus"],
                            ["schemaClass", "LiteratureReference"]
                        ] : []),

                        ...(filters.hideDatabaseReferences ? [
                            ["schemaClass", "ReferenceGeneProduct"],
                            ["schemaClass", "ReferenceDatabase"]
                        ] : []),
                    ],
                    node_property_filter_in: [
                        ...(filters.showOnlyHomoSapiens ? [
                            ["speciesName", "Homo sapiens"]
                        ] : []),
                    ],
                })
                .then(res => res.data);
            return rawElements;
        }, [dbName, nodeIds, hiddenNodeIds, filters]));
}
