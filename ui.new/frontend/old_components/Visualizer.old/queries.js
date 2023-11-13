import React from 'react';
import axios from 'axios';
import { useQuery } from "../App/queries"
import { useSelector } from 'react-redux';
import { VisualizerContext } from './context';

export const useElementsQuery = () => {
    const visualizer = React.useContext(VisualizerContext);
    const selectedNodes = useSelector(state => state.selectedNodes);
    const dbName = useSelector(state => state.dbName);
    const nodeIds = Object.keys(selectedNodes);

    return useQuery(
        ["cytoscape_elements", dbName, nodeIds, visualizer.state.hiddenNodeIds.hiddenNodeIds, visualizer.state.filters.filters],
        React.useCallback(async () => {
            const rawElements = await axios
                .post("/api/viewer/init", {
                    db_name: dbName,
                    node_ids: nodeIds,
                    hidden_node_ids: visualizer.state.hiddenNodeIds.hiddenNodeIds.map(id => id),
                    max_edge_count: 30,
                    node_property_filter_out: [
                        ...(visualizer.state.filters.filters.hideCompartments ? [
                            ["schemaClass", "Compartment"]
                        ] : []),

                        ...(visualizer.state.filters.filters.hideSpecies ? [
                            ["schemaClass", "Species"]
                        ] : []),

                        ...(visualizer.state.filters.filters.hidePublications ? [
                            ["schemaClass", "InstanceEdit"],
                            ["schemaClass", "ReviewStatus"],
                            ["schemaClass", "LiteratureReference"]
                        ] : []),

                        ...(visualizer.state.filters.filters.hideDatabaseReferences ? [
                            ["schemaClass", "ReferenceGeneProduct"],
                            ["schemaClass", "ReferenceDatabase"]
                        ] : []),
                    ],
                    node_property_filter_in: [
                        ...(visualizer.state.filters.filters.showOnlyHomoSapiens ? [
                            ["speciesName", "Homo sapiens"]
                        ] : []),
                    ],
                })
                .then(res => res.data) || [];
            return rawElements;
        }, [dbName, nodeIds, visualizer.state.hiddenNodeIds.hiddenNodeIds, visualizer.state.filters.filters]));
}
