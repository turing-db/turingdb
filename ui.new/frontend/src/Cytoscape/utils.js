import axios from 'axios';
import React from 'react';
import { useSelector } from 'react-redux';
import { useQuery } from '../App/queries';


export const useCytoscapeElements = () => {
    const dbName = useSelector((state) => state.dbName);
    const selectedNodes = useSelector((state) => state.selectedNodes);
    const displayedProperty = useSelector((state) => state.displayedProperty);
    const hiddenNodeIds = useSelector((state) => state.hiddenNodeIds);
    const nodeIds = Object.keys(selectedNodes);
    const res = useQuery(
        ["cytoscape_elements", dbName, nodeIds, hiddenNodeIds],
        React.useCallback(async () => {
            const rawElements = await axios
                .post("/api/viewer/init", {
                    db_name: dbName,
                    node_ids: nodeIds,
                    hidden_node_ids: hiddenNodeIds,
                    max_edge_count: 30,
                })
                .then(res => res.data);
            return rawElements;
        }, [dbName, nodeIds, hiddenNodeIds]))

    const data = res.data &&
        res.data.map(el => ({
            ...el,
            data: {
                ...el.data,
                label: el.group === "nodes"
                    ? el.data.properties[displayedProperty] || ""
                    : el.data.edge_type_name + "\n\u2060", // Hack to offset the labels
            }
        }));

    return { ...res, data };
}

export const useCyStyleQuery = () => useQuery(
    ["get_cystyle"],
    () => fetch('/cy-style.json')
        .then(res => res.json())
        .catch(err => console.log(err))
);


