import React from "react";
import useVisualizerState from "../state";
import { useVisualizerContext } from "../context";

const Visualizer = (props) => {
  useVisualizerState(props.cyStyle);
  const vis = useVisualizerContext();

  return (
    <div
      style={{
        position: "relative",
        overflow: "hidden",
        flexGrow: 1,
        flex: 1,
      }}>
      {props.canvas}
      {props.contextMenu}
      <div
        style={{
          position: "absolute",
          width: "100%",
          height: "100%",
          pointerEvents: "none",
        }}>
        {React.Children.map(props.children, (child) => {
          const pointerEvents = child.props.style?.pointerEvents || "auto";
          return React.cloneElement(child, {
            ...child.props,
            style: {
              ...child.props.style,
              pointerEvents: pointerEvents,
            },
          });
        })}
      </div>
    </div>
  );
};
export default Visualizer;
