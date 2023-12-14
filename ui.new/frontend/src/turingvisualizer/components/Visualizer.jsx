import React from "react";
import useVisualizerState from "../state";
import { useVisualizerContext } from "../context";

const Visualizer = (props) => {
  useVisualizerState(props.cyStyle);
  const vis = useVisualizerContext();
  //useSearchNodesDatabaseWindow({
  //  onNodeAdd: props.onNodeAdd,
  //  onNodeRemove: props.onNodeRemove,
  //  onNodeInspect: props.onNodeInspect,
  //});

  return (
    <div
      style={{
        position: "relative",
        overflow: "hidden",
        flexGrow: 1,
        flex: 1,
      }}>
      {props.canvas}
      {vis.state().devMode && (
        <div
          style={{
            position: "absolute",
            width: "100%",
            height: "100%",
            pointerEvents: "none",
            opacity: 1,
            display: "flex",
            flexDirection: "column",
            justifyContent: "end",
            paddingLeft: 20,
          }}>
          <p>Dev mode</p>
        </div>
      )}
      {props.contextMenu}
      <div
        style={{
          position: "absolute",
          width: "100%",
          pointerEvents: "none",
        }}>
        {React.Children.map(props.children, (child) =>
          React.cloneElement(child, {
            ...child.props,
            style: {
              ...child.props.style,
              pointerEvents: "auto",
            },
          })
        )}
      </div>
    </div>
  );
};
export default Visualizer;
