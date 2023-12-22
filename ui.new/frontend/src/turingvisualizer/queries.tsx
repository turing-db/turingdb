import React from "react";
import axios from "axios";
import {
  QueryFunction,
  useQuery as useNativeQuery,
} from "@tanstack/react-query";

import { Filters, getRawFilters } from "./getRawFilters";
import { GraphElement, GraphEdge, GraphNode, Property } from "./types";

export { getRawFilters };

export function useQuery<T>(
  key: any[],
  fn: QueryFunction<T, string[], never>,
  options = {}
) {
  return useNativeQuery({
    queryKey: key,
    queryFn: fn,
    staleTime: 100_000,
    refetchOnWindowFocus: false,
    refetchOnMount: false,
    ...options,
  });
}

type UseElementsQueryParams = {
  selectedNodeIds: string[];
  dbName: string;
  hiddenNodeIds: string[];
  filters: Filters;
};
export const useElementsQuery = ({
  selectedNodeIds,
  dbName,
  hiddenNodeIds,
  filters,
}: UseElementsQueryParams) =>
  useQuery(
    ["cytoscape_elements", dbName, selectedNodeIds, hiddenNodeIds, filters],
    React.useCallback(async () => {
      if (!dbName) return [];

      const { nodePropertyFilterOut, nodePropertyFilterIn } =
        getRawFilters(filters);

      const rawElements = await axios
        .post<GraphElement[]>("/api/viewer/init", {
          db_name: dbName,
          node_ids: selectedNodeIds,
          hidden_node_ids: hiddenNodeIds,
          max_edge_count: 30,
          node_property_filter_out: nodePropertyFilterOut,
          node_property_filter_in: nodePropertyFilterIn,
        })
        .then((res) => res.data);

      return rawElements;
    }, [dbName, selectedNodeIds, hiddenNodeIds, filters])
  );

export const useDevElementsQuery = () =>
  useQuery(
    ["cytoscape_dev_elements"],
    async (): Promise<GraphElement[]> =>
      fetch("/reactome-subset.json").then((res) => res.json())
  );
type UseDevElementsParams = {
  selectedNodeIds: string[];
  hiddenNodeIds: string[];
  filters: Filters;
};

export const useDevElements = ({
  selectedNodeIds,
  hiddenNodeIds,
  filters,
}: UseDevElementsParams) => {
  const { data: rawDevElements } = useDevElementsQuery();
  const devElements = rawDevElements || [];

  const { nodePropertyFilterOut, nodePropertyFilterIn } =
    getRawFilters(filters);

  if (devElements.length === 0) return { data: [] };

  const allNodes = Object.fromEntries(
    devElements
      .filter((e) => e.group === "nodes")
      .map((n) => [n.data.turing_id, n as GraphNode])
  );

  const allEdges  = Object.fromEntries(
    devElements
      .filter((e) => e.group === "edges")
      .map((e) => [e.data.turing_id, e as GraphEdge])
  );

  // Selected nodes
  const selectedNodesMap = Object.fromEntries(
    selectedNodeIds.map((id) => [id, allNodes[id]!])
  );

  for (const n of Object.values(selectedNodesMap)) {
    n.data.type = "selected";
  }

  // Connecting edges
  const connectingEdgesMap = Object.fromEntries(
    Object.values(allEdges)
      .filter(
        (e) =>
          selectedNodesMap[e.data.turing_source_id] &&
          selectedNodesMap[e.data.turing_target_id]
      )
      .map((e) => [e.data.turing_id, e])
  );

  for (const e of Object.values(connectingEdgesMap)) {
    e.data.type = "connecting";
  }

  // Neighbor edges are edges that are connected to only one selected node
  // + that don't imply hidden nodes
  // + that only imply nodes that respect the filters
  const hiddenNodeIdMap = Object.fromEntries(
    hiddenNodeIds.map((id) => [id, true])
  );

  const allNeighborEdgesMap = Object.fromEntries(
    Object.values(allEdges)
      .filter((e) => {
        const tsid = e.data.turing_source_id;
        const ttid = e.data.turing_target_id;
        const sourceIsSelected = selectedNodesMap[tsid] !== undefined;
        const targetIsSelected = selectedNodesMap[ttid] !== undefined;

        const impliesOnlyOneSelectedNode =
          sourceIsSelected !== targetIsSelected;
        const impliesHiddenNode =
          hiddenNodeIdMap[tsid] || hiddenNodeIdMap[ttid];

        return impliesOnlyOneSelectedNode && !impliesHiddenNode;
      })
      .map((e) => [e.data.turing_id, e])
  );

  const allNeighborNodesMap = Object.fromEntries(
    [
      ...new Set(
        Object.values(allNeighborEdgesMap) // Set for unique ids
          .map((e) => [e.data.turing_source_id, e.data.turing_target_id])
          .flat()
          .filter((nId) => !selectedNodesMap[nId]) // Not selected
      ),
    ].map((nId) => [nId, allNodes[nId]!])
  );

  const filteredNeighborNodesMap = Object.fromEntries(
    Object.values(allNeighborNodesMap)
      .filter((n) => {
        const filterOutOk =
          nodePropertyFilterOut.length === 0
            ? [true]
            : nodePropertyFilterOut.every(([pName, pValue]) => {
                const v = n.data.properties[pName];
                const hasProperty = v !== undefined;
                return !hasProperty || v !== pValue;
              });

        const filterInOk =
          nodePropertyFilterIn.length === 0
            ? [true]
            : nodePropertyFilterIn.some(([pName, pValue]) => {
                const v = n.data.properties[pName];
                const hasProperty = v !== undefined;
                return !hasProperty || v === pValue;
              });

        return filterOutOk && filterInOk;
      })
      .map((n) => [n.data.turing_id, n])
  );

  const filteredNeighborEdgesMap = Object.fromEntries(
    Object.values(allNeighborEdgesMap)
      .filter((e) => {
        const tsid = e.data.turing_source_id;
        const ttid = e.data.turing_target_id;
        const sourceValid =
          filteredNeighborNodesMap[tsid] || selectedNodesMap[tsid];
        const targetValid =
          filteredNeighborNodesMap[ttid] || selectedNodesMap[ttid];

        return sourceValid && targetValid;
      })
      .map((e) => [e.data.turing_id, e])
  );

  const importantNodeCounts = Object.fromEntries(
    Object.keys(filteredNeighborNodesMap).map((id) => [id, 0])
  );

  for (const e of Object.values(filteredNeighborEdgesMap)) {
    if (selectedNodesMap[e.data.turing_source_id] === undefined)
      importantNodeCounts[e.data.turing_source_id] += 1;
    if (selectedNodesMap[e.data.turing_target_id] === undefined)
      importantNodeCounts[e.data.turing_target_id] += 1;
  }

  const importantNodesMap = Object.fromEntries(
    Object.entries(importantNodeCounts)
      .filter(([, count]) => count > 1)
      .map(([id]) => [id, filteredNeighborNodesMap[id]!])
  );

  const importantEdgesMap = Object.fromEntries(
    Object.entries(filteredNeighborEdgesMap).filter(([, e]) => {
      const sourceIsImportant =
        importantNodesMap[e.data.turing_source_id] !== undefined;
      const targetIsImportant =
        importantNodesMap[e.data.turing_target_id] !== undefined;

      return sourceIsImportant || targetIsImportant;
    })
  );

  const edgeCounts = Object.fromEntries(
    Object.keys(allNodes).map((id) => [id, 0])
  );

  const neighborEdgesMap = {
    ...Object.fromEntries(
      Object.entries(filteredNeighborEdgesMap).filter(([, e]) => {
        const tsid = e.data.turing_source_id;
        const ttid = e.data.turing_target_id;

        edgeCounts[tsid] += 1;
        edgeCounts[ttid] += 1;

        if (edgeCounts[tsid]! > 20 || edgeCounts[ttid]! > 20) {
          return false;
        }

        return true;
      })
    ),
    ...importantEdgesMap,
  };

  const neighborNodesMap = {
    ...Object.fromEntries(
      [
        ...new Set(
          Object.values(neighborEdgesMap) // Set for unique ids
            .map((e) => [e.data.turing_source_id, e.data.turing_target_id])
            .flat()
        ),
      ]
        .filter((id) => selectedNodesMap[id] === undefined)
        .map((nId) => [nId, filteredNeighborNodesMap[nId]!])
    ),
    ...importantNodesMap,
  };

  for (const e of Object.values(neighborEdgesMap)) {
    e.data.type = "neighbor";
  }

  for (const e of Object.values(neighborNodesMap)) {
    e.data.type = "neighbor";
  }

  const rawElements = [
    ...Object.values(selectedNodesMap),
    ...Object.values(connectingEdgesMap),

    ...Object.values(neighborNodesMap),
    ...Object.values(neighborEdgesMap),
  ];

  return { data: rawElements };
};

export type ListNodesDevParams = {
  filters: Filters;
  additionalPropFilterOut: Property[];
  additionalPropFilterIn: Property[];
};

export type ListEdgesDevParams = {
  nodeId: string;
  nodeFilters: Filters;
  edgeTypeNames: string[];
  additionalNodePropFilterOut: Property[];
  additionalNodePropFilterIn: Property[];
  edgePropFilterOut: Property[];
  edgePropFilterIn: Property[];
};

export type GetNodesDevParams = {
  nodeIds: string[];
};

export type GetEdgesDevParams = {
  edgeIds: string[];
};

export const devEndpoints = {
  list_nodes: async ({
    filters = {},
    additionalPropFilterOut = [],
    additionalPropFilterIn = [],
  }: ListNodesDevParams) => {
    const { data: rawDevElements } = useDevElementsQuery();
    const devElements = rawDevElements || [];

    const { nodePropertyFilterOut, nodePropertyFilterIn } =
      getRawFilters(filters);

    const filterOut = [...nodePropertyFilterOut, ...additionalPropFilterOut];

    const filterIn = [...nodePropertyFilterIn, ...additionalPropFilterIn];

    return devElements
      .filter((e) => e.group === "nodes")
      .map((n) => n as GraphNode)
      .filter((n) => {
        const filterOutOk =
          filterOut.length === 0 ||
          filterOut.every(([pName, pValue]) => {
            const v = n.data.properties[pName];
            const hasProperty = v !== undefined;
            return !hasProperty || v !== pValue;
          });

        const filterInOk =
          filterIn.length === 0 ||
          filterIn.every(([pName, pValue]) => {
            const v = n.data.properties[pName];
            const hasProperty = v !== undefined;
            return !hasProperty || v.includes(pValue);
          });

        return filterOutOk && filterInOk;
      });
  },

  list_edges: async ({
    nodeId,
    nodeFilters = {},
    edgeTypeNames = [],
    additionalNodePropFilterOut = [],
    additionalNodePropFilterIn = [],
    edgePropFilterOut = [],
    edgePropFilterIn = [],
  }: ListEdgesDevParams) => {
    const { data: rawDevElements } = useDevElementsQuery();
    const devElements = rawDevElements || [];

    const mappedDevNodes = Object.fromEntries(
      devElements
        .filter((e) => e.group === "nodes")
        .map((e) => [e.data.turing_id, e])
    );
    const mappedEdgeTypeNames = Object.fromEntries(
      edgeTypeNames.map((name) => [name, true])
    );

    const { nodePropertyFilterOut, nodePropertyFilterIn } =
      getRawFilters(nodeFilters);

    const nodeFilterOut = [
      ...nodePropertyFilterOut,
      ...additionalNodePropFilterOut,
    ];

    const nodeFilterIn = [
      ...nodePropertyFilterIn,
      ...additionalNodePropFilterIn,
    ];

    return devElements
      .filter((e) => e.group === "edges")
      .map((e) => e as GraphEdge)
      .filter(
        (e) =>
          e.data.turing_source_id === nodeId ||
          e.data.turing_target_id === nodeId
      )
      .filter(
        (e) =>
          edgeTypeNames.length === 0 ||
          mappedEdgeTypeNames[e.data.edge_type_name!]
      )
      .filter((e) => {
        const filterOutOk =
          edgePropFilterOut.length === 0 ||
          edgePropFilterOut.every(([pName, pValue]) => {
            const v = e.data.properties[pName];
            const hasProperty = v !== undefined;
            return !hasProperty || v.toString() !== pValue;
          });

        const filterInOk =
          edgePropFilterIn.length === 0 ||
          edgePropFilterIn.every(([pName, pValue]) => {
            const v = e.data.properties[pName];
            const hasProperty = v !== undefined;
            return !hasProperty || v.toString().includes(pValue);
          });

        return filterOutOk && filterInOk;
      })
      .filter((e) => {
        const otherNode =
          e.data.turing_source_id === nodeId
            ? mappedDevNodes[e.data.turing_target_id]!
            : mappedDevNodes[e.data.turing_source_id]!;

        const filterOutOk =
          nodeFilterOut.length === 0 ||
          nodeFilterOut.every(([pName, pValue]) => {
            const v = otherNode.data.properties[pName];
            const hasProperty = v !== undefined;
            return !hasProperty || v !== pValue;
          });

        const filterInOk =
          nodeFilterIn.length === 0 ||
          nodeFilterIn.every(([pName, pValue]) => {
            const v = otherNode.data.properties[pName];
            const hasProperty = v !== undefined;
            return !hasProperty || v.includes(pValue);
          });

        return filterOutOk && filterInOk;
      });
  },

  get_nodes: async ({ nodeIds }: GetNodesDevParams) => {
    const { data: rawDevElements } = useDevElementsQuery();
    const devElements = rawDevElements || [];

    const mappedIds = Object.fromEntries(nodeIds.map((id) => [id, true]));

    return devElements.filter(
      (e) => mappedIds[e.data.turing_id] && e.group === "nodes"
    ).map((n) => n as GraphNode);
  },

  get_edges: async ({ edgeIds }: GetEdgesDevParams) => {
    const { data: rawDevElements } = useDevElementsQuery();
    const devElements = rawDevElements || [];

    const mappedIds = Object.fromEntries(edgeIds.map((id) => [id, true]));

    return devElements.filter(
      (e) => mappedIds[e.data.turing_id] && e.group === "edges"
    ).map((e) => e as GraphEdge);
  },
};
