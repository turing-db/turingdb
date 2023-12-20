// Core
import React from "react";
import { useVisualizerContext } from "../../../context";

// @blueprintjs
import { Tooltip, MenuItem, Button } from "@blueprintjs/core";
import { SelectMenu } from "../../ContextMenu/select";
import { lockBehaviours } from "../../../reducers/layouts";

const ttParams = {
  hoverCloseDelay: 40,
  hoverOpenDelay: 400,
};

const LockMenu = () => {
  const vis = useVisualizerContext();
  const lockValues = ["Main nodes", "Interacted nodes", "None"];
  const [lock, setLock] = React.useState("Interacted nodes");

  React.useEffect(() => {
    let lockValue = 0;

    switch (lock) {
      case "Main nodes": {
        lockValue = lockBehaviours.ALL_SELECTED;
        break;
      }
      case "Interacted nodes": {
        lockValue = lockBehaviours.INTERACTED;
        break;
      }
      case "None": {
        lockValue = lockBehaviours.DO_NOT_LOCK;
        break;
      }
    }

    if (vis.state().layouts.definitions[0].lockBehaviour !== lockValue) {
      vis.callbacks().setDefaultCyLayout({
        ...vis.state().layouts.definitions[0],
        lockBehaviour: lockValue,
      });
    }
  }, [vis, lock]);

  const renderValue = (behaviour) => {
    return (
      <MenuItem
        key={"lock-behaviour-" + behaviour}
        text={behaviour}
        onClick={() => setLock(behaviour)}
      />
    );
  };

  return (
    <div className="flex items-center space-x-2">
      <div>Lock nodes during layout run</div>
      <Tooltip content="Lock nodes during layout run" {...ttParams}>
        <SelectMenu
          key={"select-menu-set-node-label"}
          renderChild={renderValue}
          items={lockValues}
          placement={"bottom"}
          Parent={
            <Button
              key="set-node-label"
              fill
              style={{ width: "180px" }}
              alignText="left"
              icon="lock"
              text={lock}
              rightIcon="caret-down"
            />
          }
        />
      </Tooltip>
    </div>
  );
};

export default LockMenu;
