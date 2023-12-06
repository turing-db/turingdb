import React from "react";
import axios from "axios";
//import { useVisualizerContext } from "../../context";
import { useDialog } from "../../dialogs";
import { useSelector } from "react-redux";
import { SelectProperty } from "./SelectProperty";

import { Tag, InputGroup, Button, Card, Spinner } from "@blueprintjs/core";
import { getRawFilters } from "../../queries";
import { useVisualizerContext } from "../../context";

const NODE_SCORES = {
  EXACT_SAME_CASE_PROP_MATCH: 10000,
  EXACT_PROP_MATCH: 1000,
  SAME_CASE_PROP_MATCH: 10,
  PROP_MATCH: 10,
  ID_MATCH: 1,
  EXACT_ID_MATCH: 20000,
};

const useSearchNodesDatabaseWindow = (props) => {
  const dbName = useSelector((state) => state.dbName);
  const properties = usePropertyTypes(dbName);
  const [currentPropName, setCurrentPropName] = React.useState();
  const [currentPropValue, setCurrentPropValue] = React.useState();
  const selectedNodes = useSelector((state) => state.selectedNodes);
  //const [currentSortValue, setCurrentSortValue] = React.useState<string>("");
  const { loading, nodes, query } = useNodes(
    dbName,
    currentPropName,
    currentPropValue
  );
  const nodeScores = React.useMemo(
    () =>
      Object.fromEntries(
        nodes.map((n) => {
          const prop = n.properties[currentPropName || ""] || "";
          const input = currentPropValue || "";

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
    [nodes, currentPropName, currentPropValue]
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

  useDialog({
    name: "search-nodes-in-database",
    title: "Find nodes in database",
    width: "80%",
    dialogProps: props,
    content: (contentProps) => {
      return (
        <div className="h-[65vh]">
          <form
            onSubmit={(e) => {
              e.preventDefault();
              query();
            }}>
            <div className="flex space-x-2 items-center p-6 pt-4 pb-4">
              <div>Find by property</div>
              <SelectProperty
                currentProp={currentPropName}
                setCurrentProp={setCurrentPropName}
                properties={properties}
              />
              <InputGroup
                className="flex-1"
                onChange={(e) => setCurrentPropValue(e.target.value)}
              />
              <Button type="submit" icon="search" />
            </div>
          </form>
          <div className="w-full h-full overflow-auto">
            {loading ? (
              <div className="flex h-full w-full items-center justify-center">
                <Spinner size={100} />
              </div>
            ) : (
              sortedNodes.map((n) => {
                const isSelected = selectedNodes[n.id] !== undefined;

                return (
                  <Card
                    key={n.id}
                    interactive
                    compact
                    className="flex justify-between"
                    onClick={(e) => {
                      e.preventDefault();
                    }}>
                    <div className="flex space-x-2">
                      <Tag className="w-20 text-center">{n.id}</Tag>
                      <div className={isSelected ? "primary-light" : ""}>
                        {getNodePropValue(n, currentPropName)}
                      </div>
                    </div>
                    {isSelected ? (
                      <Button
                        icon="remove"
                        onClick={(e) => {
                          e.preventDefault();
                          contentProps.onNodeRemove(n);
                          e.stopPropagation();
                        }}
                      />
                    ) : (
                      <Button
                        icon="add"
                        onClick={(e) => {
                          e.preventDefault();
                          contentProps.onNodeAdd(n);
                          e.stopPropagation();
                        }}
                      />
                    )}
                  </Card>
                );
              })
            )}
          </div>
        </div>
      );
    },
  });
};

function useNodes(dbName, propName, propValue) {
  const [nodes, setNodes] = React.useState([]);
  const [loading, setLoading] = React.useState(false);
  const vis = useVisualizerContext();

  const query = React.useCallback(
    () =>
      queryNodes({
        propName: propName || "",
        propValue: propValue || "",
        setLoading,
        filters: vis.state().filters,
        dbName,
        setNodes,
      }),
    [vis, dbName, propName, propValue, setLoading, setNodes]
  );
  return { loading, nodes, query };
}

function queryNodes(params) {
  if (!params.propName || !params.propValue) return;

  params.setLoading(true);
  const { nodePropertyFilterIn, nodePropertyFilterOut } = getRawFilters(
    params.filters
  );

  axios
    .post("/api/list_nodes", {
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
        res.data.map((n) => {
          return {
            id: String(n.id),
            properties: n.properties,
          };
        })
      );
    });
}

function usePropertyTypes(dbName) {
  const [props, setProps] = React.useState([]);

  React.useEffect(() => {
    axios
      .post("/api/list_node_property_types", { db_name: dbName })
      .then((res) => {
        if (res.data.error) {
          setProps([]);
          return;
        }
        setProps(res.data);
      })
      .catch((err) => {
        console.log(err);
        setProps([]);
      });
  }, [dbName]);

  return props;
}

const propValueLim = 40;
function getNodePropValue(n, propName) {
  const v = n.properties[propName || ""] || "";
  if (v.length > propValueLim) return v.slice(0, propValueLim - 3) + "...";
  return v;
}

export default useSearchNodesDatabaseWindow;
