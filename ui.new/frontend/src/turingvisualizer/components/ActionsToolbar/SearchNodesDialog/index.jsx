// Core
import React from "react";

// @blueprintjs
import { Button, Dialog, Tooltip } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "../../../";
import { dialogParams, ttParams, useDefinedState } from "../tools";

import QueryNodesBar from "./QueryNodesBar";
import NodeContainer from "./NodeContainer";
import { usePropertyTypes } from "../AddNodeDialog/nodes";

export function SearchNodesDialogButton() {
  const vis = useVisualizerContext();
  return (
    <Tooltip {...ttParams} content="Search nodes in current view">
      <Button
        icon="search"
        text="Search view"
        onClick={vis.searchNodesDialog.open}
      />
    </Tooltip>
  );
}

export function SearchNodesDialog() {
  const vis = useVisualizerContext();

  const propName = useDefinedState("");
  const [propValue, setPropValue] = React.useState("");
  const properties = usePropertyTypes(vis.state().dbName);

  const previousNodes = React.useRef([]);

  React.useEffect(() => {
    if (properties.includes("displayName")) {
      propName.setOnce("displayName");
      return;
    }
    if (properties.includes("label")) {
      propName.setOnce("label");
      return;
    }
  }, [properties, propName.setOnce]);

  const nodes = vis.searchNodesDialog.isOpen
    ? vis
        .cy()
        ?.nodes()
        .filter((n) => {
          const v = String(
            (() => {
              if (propName.value === "None") return n.data().turing_id;
              if (propName.value === "Node Type")
                return n.data().node_type_name;
              return n.data().properties[propName.value] || "";
            })()
          );

          return v.toLowerCase().includes(propValue.toLowerCase());
        })
    : previousNodes.current;
  previousNodes.current = nodes;

  const componentProps = {
    propName: propName.value,
    setPropName: propName.set,
    propValue,
    setPropValue,
    propNames: properties,
    nodes,
  };

  return (
    <>
      <Dialog
        {...dialogParams}
        isOpen={vis.searchNodesDialog.isOpen}
        onClose={vis.searchNodesDialog.close}
        className={dialogParams.className + " h-[70vh]"}
        title="Search nodes in graph">
        <QueryNodesBar {...componentProps} />
        <NodeContainer {...componentProps} />
      </Dialog>
    </>
  );
}
