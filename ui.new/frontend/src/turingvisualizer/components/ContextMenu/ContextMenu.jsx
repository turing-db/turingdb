// Core
import React from "react";

// @blueprintjs
import { ContextMenu as BPContextMenu } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "../../";

const RawContextMenu = React.forwardRef((props, ref) => {
  return (
    <BPContextMenu popoverProps={{}} content={props.content}>
      {(ctxMenuProps) => (
        <div onContextMenu={ctxMenuProps.onContextMenu} ref={ref}>
          {ctxMenuProps.popover}
          {ctxMenuProps.children}
        </div>
      )}
    </BPContextMenu>
  );
});

export const useContextMenuData = () => {
  const vis = useVisualizerContext();
  const [data, setData] = React.useState({});
  vis.refs.contextMenu.data.current = data;
  vis.refs.contextMenu.setData.current = setData;
};

export const ContextMenu = (props) => {
  const vis = useVisualizerContext();

  return (
    <RawContextMenu
      ref={vis.refs.contextMenu.ref}
      content={<div
        className={vis.state().themeMode === "dark" ? "bp5-dark" : "bp5"}>
        {props.children}
      </div>}
    />
  );
};
