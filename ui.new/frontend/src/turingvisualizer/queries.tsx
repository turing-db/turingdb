import React from "react";
import axios from "axios";
import {
  QueryFunction,
  useQuery as useNativeQuery,
} from "@tanstack/react-query";

import { Filters, getRawFilters } from "./getRawFilters";
import {
  GraphElement,
  GraphEdge,
  GraphNode,
  Property,
  GraphNodeData,
  GraphEdgeData,
} from "./types";

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

export type ListNodesDevParams = {
  dbName?: string;
  filters?: Filters;
  additionalPropFilterOut?: Property[];
  additionalPropFilterIn?: Property[];
  yield_edges?: boolean;
};

export type ListEdgesDevParams = {
  dbName?: string;
  nodeId: string;
  nodeFilters?: Filters;
  edgeTypeNames?: string[];
  additionalNodePropFilterOut?: Property[];
  additionalNodePropFilterIn?: Property[];
  edgePropFilterOut?: Property[];
  edgePropFilterIn?: Property[];
};

export type ListPathwaysDevParams = {
  dbName?: string;
  nodeId: string;
};

export type GetPathwayDevParams = {
  dbName?: string;
  nodeId: string;
};

export type GetNodesDevParams = {
  dbName?: string;
  nodeIds: string[];
  yield_edges: boolean;
};

export type GetEdgesDevParams = {
  dbName?: string;
  edgeIds: string[];
};

export type Endpoints = {
  list_nodes: typeof prod_list_nodes;
  list_edges: typeof prod_list_edges;
  get_nodes: typeof prod_get_nodes;
  get_edges: typeof prod_get_edges;
  list_pathways: typeof prod_list_pathways;
  get_pathway: typeof prod_get_pathway;
};

export const endpoints: Endpoints = {
  list_nodes: prod_list_nodes,
  list_edges: prod_list_edges,
  get_nodes: prod_get_nodes,
  get_edges: prod_get_edges,
  list_pathways: prod_list_pathways,
  get_pathway: prod_get_pathway,
};

type ListNodesRes = {
  failed: boolean;
  error: { details: string };
  nodes: GraphNodeData[];
};

type ListEdgesRes = {
  failed: boolean;
  error: { details: string };
  edges: GraphEdgeData[];
};

type ListPathwaysRes = {
  failed: boolean;
  error: { details: string };
  nodes: GraphNodeData[];
};

type GetPathwayRes = {
  failed: boolean;
  error: { details: string };
  nodes: GraphNodeData[];
};

type GetPathwayResult = {
  failed: boolean;
  error?: string;
  nodes: GraphNode[];
};

async function prod_list_nodes({
  dbName = "reactome",
  filters = {},
  additionalPropFilterOut = [],
  additionalPropFilterIn = [],
  yield_edges = false,
}: ListNodesDevParams): Promise<GraphNode[]> {
  const { nodePropertyFilterOut, nodePropertyFilterIn } =
    getRawFilters(filters);
  const filterOut = [...nodePropertyFilterOut, ...additionalPropFilterOut];
  const filterIn = [...nodePropertyFilterIn, ...additionalPropFilterIn];

  return await axios
    .post<ListNodesRes>("/api/list_nodes", {
      db_name: dbName,
      node_property_filter_out: filterOut,
      node_property_filter_in: filterIn,
      yield_edges,
    })
    .then((res) => {
      if (res.data.failed) {
        return [];
      }
      return res.data.nodes.map((n) => {
        return {
          group: "nodes",
          data: {
            ...n,
            id: String(n.id + 1),
            turing_id: String(n.id),
          },
        };
      });
    });
}

async function prod_list_edges({
  dbName = "reactome",
  nodeId,
  edgeTypeNames = [],
  edgePropFilterOut = [],
  edgePropFilterIn = [],
}: ListEdgesDevParams) {
  const node = (
    await prod_get_nodes({
      dbName,
      nodeIds: [nodeId],
      yield_edges: true,
    })
  )[0]!;

  const ins = node.data.ins!;
  const outs = node.data.outs!;
  let edges: GraphEdge[] = await axios
    .post<ListEdgesRes>("/api/list_edges", {
      db_name: dbName,
      ids: [...ins, ...outs],
    })
    .then((res) => {
      return res.data.edges.map((e) => {
        return {
          group: "edges",
          data: {
            ...e,
            id: String(e.id + 1),
            turing_id: String(e.id),
            turing_source_id: String(e.source_id),
            turing_target_id: String(e.target_id),
          },
        };
      });
    });

  edges = edges.filter((e) => {
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
  });

  const mappedEdgeTypeNames = Object.fromEntries(
    edgeTypeNames.map((name) => [name, true])
  );

  return edges.filter(
    (e) =>
      edgeTypeNames.length === 0 || mappedEdgeTypeNames[e.data.edge_type_name!]
  );
}

async function prod_get_nodes({
  dbName = "reactome",
  nodeIds,
  yield_edges = false,
}: GetNodesDevParams): Promise<GraphNode[]> {
  return await axios
    .post<ListNodesRes>("/api/list_nodes", {
      db_name: dbName,
      ids: nodeIds,
      yield_edges,
    })
    .then((res) => {
      if (res.data.failed) {
        return [];
      }
      return res.data.nodes.map((n) => {
        return {
          group: "nodes",
          data: {
            ...n,
            id: String(n.id + 1),
            turing_id: String(n.id),
          },
        };
      });
    });
}

async function prod_get_edges({
  dbName = "reactome",
  edgeIds,
}: GetEdgesDevParams): Promise<GraphEdge[]> {
  return await axios
    .post<ListEdgesRes>("/api/list_edges", {
      db_name: dbName,
      ids: edgeIds,
    })
    .then((res) => {
      if (res.data.failed) {
        return [];
      }
      return res.data.edges.map((e) => {
        return {
          group: "edges",
          data: {
            ...e,
            id: String(e.id + 1),
            turing_id: String(e.id),
            turing_source_id: String(e.source_id),
            turing_target_id: String(e.target_id),
          },
        };
      });
    });
}

async function prod_list_pathways({
  nodeId,
  dbName,
}: ListPathwaysDevParams): Promise<GraphNode[]> {
  return await axios
    .post<ListPathwaysRes>("/api/list_pathways", {
      db_name: dbName,
      node_id: nodeId,
    })
    .then((res) => {
      if (res.data.failed) {
        return [];
      }
      return res.data.nodes.map((n) => {
        return {
          group: "nodes",
          data: {
            ...n,
            id: String(n.id + 1),
            turing_id: String(n.id),
          },
        };
      });
    });
}

async function prod_get_pathway({
  nodeId,
  dbName,
}: GetPathwayDevParams): Promise<GetPathwayResult> {
  return await axios
    .post<GetPathwayRes>("/api/get_pathway", {
      db_name: dbName,
      node_id: nodeId,
    })
    .then((res) => {
      if (res.data.failed) {
        return {
          failed: true,
          error: res.data.error.details,
          nodes: [],
        };
      }
      return {
        nodes: res.data.nodes.map((n) => {
          return {
            group: "nodes",
            data: {
              ...n,
              id: String(n.id + 1),
              turing_id: String(n.id),
            },
          };
        }),
        failed: false,
      };
    });
}
