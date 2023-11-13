import React from "react";
import useVisualizerState from "./state";

const Visualizer = (props) => {
  useVisualizerState(props.cyStyle);

  return (
    <>
      <div
        style={{
          flexGrow: 1,
          position: "relative",
          overflow: "hidden",
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
          {React.Children.map(props.children, (child) =>
            React.cloneElement(child, {
              style: {
                ...child.props.style,
                pointerEvents: "auto",
              },
            })
          )}
        </div>
      </div>
    </>
  );
};
export default Visualizer;
