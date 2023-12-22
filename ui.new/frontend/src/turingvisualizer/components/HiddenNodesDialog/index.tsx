import React from "react";
import { useVisualizerContext } from "../../context";
import { useCanvasTrigger } from "../../useCanvasTrigger";
import { dialogParams, ttParams, useDialog } from "../../tools";
import { Tag, CardList, Card, Button, Dialog, Tooltip } from "@blueprintjs/core";

export default function HiddenNodesDialog() {
  const vis = useVisualizerContext();
  const dialog = useDialog();

  const [hiddenNodes, setHiddenNodes] = React.useState(
    vis.state()!.hiddenNodes
  );

  useCanvasTrigger({
    category: "hiddenNodes",
    name: "hiddenNodesModal-setState",
    callback: () => {
      setHiddenNodes({ ...vis.state()!.hiddenNodes });
    },
  });

  return (
    <>
      <Tooltip {...ttParams} content="Hidden nodes">
        <Button icon="eye-open" onClick={dialog.open} />
      </Tooltip>
      <Dialog
        {...dialogParams}
        title="Hidden nodes"
        isOpen={dialog.isOpen}
        onClose={dialog.close}>
        <CardList className="h-[60vh]">
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
                    vis.callbacks()!.showNodes([n.turing_id]);
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
                    {vis.state()!.nodeLabel === "Node Type"
                      ? n.node_type_name
                      : n.properties[vis.state()!.nodeLabel] || "Unnamed node"}
                  </div>
                </div>
              </Card>
            ))
          ) : (
            <div>No hidden nodes</div>
          )}
        </CardList>
      </Dialog>
    </>
  );
}
