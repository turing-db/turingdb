// Core
import React from "react";

// @blueprintjs
import { Card, CardList, Tag, Button } from "@blueprintjs/core";

// Turing
import { useVisualizerContext, useCanvasTrigger } from "../../";
import { useDialog } from "src/turingvisualizer/dialogs";

const useHiddenNodesPopover = () => {
  const vis = useVisualizerContext();
  const [hiddenNodes, setHiddenNodes] = React.useState(vis.state().hiddenNodes);

  useCanvasTrigger({
    category: "hiddenNodes",
    name: "hiddenNodesModal-setState",
    callback: () => {
      setHiddenNodes({ ...vis.state().hiddenNodes });
    },
  });

  useDialog({
    name: "hidden-nodes",
    title: "Hidden nodes",
    content: () => {
      return (
        <>
          <CardList
            style={{
              maxHeight: "50vh",
              overflow: "auto",
            }}>
            {Object.values(hiddenNodes).length !== 0 ? (
              Object.values(hiddenNodes).map((n) => (
                <Card
                  key={"node-" + n.turing_id}
                  elevation={2}
                  style={{
                    display: "flex",
                    flexDirection: "row",
                    alignItems: "center",
                    justifyContent: "start",
                  }}>
                  <Button
                    icon="eye-open"
                    small
                    style={{ marginRight: 10 }}
                    onClick={() => {
                      vis.callbacks().showNodes([n.turing_id]);
                    }}
                  />
                  <div
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      alignItems: "start",
                    }}>
                    <Tag minimal style={{ width: "min-content" }}>
                      id [{n.turing_id}]
                    </Tag>
                    <div>
                      {vis.state().nodeLabel === "Node Type"
                        ? n.node_type_name
                        : n.properties[vis.state().nodeLabel] || "Unnamed node"}
                    </div>
                  </div>
                </Card>
              ))
            ) : (
              <div>No hidden nodes</div>
            )}
          </CardList>
        </>
      );
    },
  });
};

export default useHiddenNodesPopover;
