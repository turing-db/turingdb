import React from "react";
import axios from "axios";
import { Filters, getRawFilters } from "../../getRawFilters";
import { useVisualizerContext } from "../../context";
import { GraphNodeData } from "../../types";

const NODE_SCORES = {
  EXACT_SAME_CASE_PROP_MATCH: 10000,
  EXACT_PROP_MATCH: 1000,
  SAME_CASE_PROP_MATCH: 10,
  PROP_MATCH: 10,
  ID_MATCH: 1,
  EXACT_ID_MATCH: 20000,
};

type ListNodePropertyTypesRes = { error: string; data: string[] };

type QueryNodeParams = {
  dbName: string;
  propName?: string;
  propValue?: string;
  setLoading: React.Dispatch<React.SetStateAction<boolean>>;
  filters: Filters;
  setNodes: React.Dispatch<React.SetStateAction<GraphNodeData[]>>;
};

type ListNodesRes = {
  failed: boolean;
  error: { details: string };
  nodes: GraphNodeData[];
};

export function useNodes(
  dbName: string,
  propName: string | undefined,
  propValue: string | undefined
) {
  const [nodes, setNodes] = React.useState<GraphNodeData[]>([]);
  const [loading, setLoading] = React.useState(false);
  const vis = useVisualizerContext();

  const query = React.useCallback(
    () =>
      queryNodes({
        propName: propName || "",
        propValue: propValue || "",
        setLoading,
        filters: vis.state()!.filters,
        dbName,
        setNodes,
      }),
    [vis, dbName, propName, propValue, setLoading, setNodes]
  );

  const nodeScores = React.useMemo(
    () =>
      Object.fromEntries(
        nodes.map((n) => {
          const prop = n.properties[propName || ""] || "";
          const input = propValue || "";

          const exactSameCasePropMatch =
            prop === input ? NODE_SCORES.EXACT_SAME_CASE_PROP_MATCH : 0;

          const exactPropMatch =
            prop.toLowerCase() === input.toLowerCase()
              ? NODE_SCORES.EXACT_PROP_MATCH
              : 0;

          const sameCasePropMatch = prop.includes(input)
            ? NODE_SCORES.SAME_CASE_PROP_MATCH
            : 0;

          const propMatch = prop.toLowerCase().includes(input.toLowerCase())
            ? NODE_SCORES.PROP_MATCH
            : 0;

          const exactIdMatch = n.id === input ? NODE_SCORES.EXACT_ID_MATCH : 0;
          const idMatch = n.id.includes(input) ? NODE_SCORES.ID_MATCH : 0;

          return [
            n.id,
            exactSameCasePropMatch +
              exactPropMatch +
              sameCasePropMatch +
              propMatch +
              exactIdMatch +
              idMatch,
          ];
        })
      ),
    [nodes, propName, propValue]
  );

  const sortedNodes = React.useMemo(
    () =>
      nodes.sort((a, b) => {
        const scoreA = nodeScores[a.id] || 0;
        const scoreB = nodeScores[b.id] || 0;
        return scoreB - scoreA;
      }),
    [nodes, nodeScores]
  );

  return { loading, nodes, nodeScores, sortedNodes, query };
}

function queryNodes(params: QueryNodeParams) {
  if (!params.propName || !params.propValue) return;

  params.setLoading(true);
  const { nodePropertyFilterIn, nodePropertyFilterOut } = getRawFilters(
    params.filters
  );

  axios
    .post<ListNodesRes>("/api/list_nodes", {
      db_name: params.dbName,
      node_property_filter_out: nodePropertyFilterOut,
      node_property_filter_in: nodePropertyFilterIn,
      prop_name: params.propName,
      prop_value: params.propValue,
      yield_edges: false,
    })
    .then((res) => {
      params.setLoading(false);

      if (res.data.failed) {
        console.log("list_nodes failed ", res.data.error.details);
        params.setNodes([]);
        return;
      }

      params.setNodes(
        res.data.nodes.map((n) => {
          return {
            ...n,
            id: String(n.id + 1),
            turing_id: String(n.id),
          };
        })
      );
    });
}

export function usePropertyTypes(dbName: string) {
  const [props, setProps] = React.useState<string[]>([]);

  React.useEffect(() => {
    axios
      .post<ListNodePropertyTypesRes>("/api/list_node_property_types", {
        db_name: dbName,
      })
      .then((res) => {
        if (res.data.error) {
          setProps([]);
          return;
        }
        setProps(res.data.data);
      })
      .catch((err) => {
        console.log(err);
        setProps([]);
      });
  }, [dbName]);

  return props;
}

const propValueLim = 40;
export function getNodePropValue(n: GraphNodeData, propName: string) {
  const v = n.properties[propName || ""] || "";
  if (v.length > propValueLim) return v.slice(0, propValueLim - 3) + "...";
  return v;
}

export function getNodeSecondaryPropValue(n: GraphNodeData, propName: string) {
  const secondaryPropName =
    propName === "displayName" ? "schemaClass" : "displayName";
  const v = n.properties[secondaryPropName || ""] || "";
  if (v.length > propValueLim) return v.slice(0, propValueLim - 3) + "...";
  return v;
}
