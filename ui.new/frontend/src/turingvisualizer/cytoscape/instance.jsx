// Core
import React from "react";
import cytoscape from "cytoscape";
import cola from "cytoscape-cola";

// Turing
import { style, useVisualizerContext } from "..";

if (typeof cytoscape("core", "cola") === "undefined") {
  cytoscape.use(cola);
}

export const useCytoscapeInstance = () => {
  const vis = useVisualizerContext();
  const cytoscapeProps = vis.state().cytoscapeProps;

  React.useEffect(() => {
    if (vis.cy()) return;

    vis.refs.cy.current = cytoscape({
      container: document.getElementById(vis.containerId),
      style: style,
    });

    const evs = vis.refs.events;
    vis.cy().on("onetap", (e) => evs.current.onetap(vis, e));
    vis.cy().on("cxttap", (e) => evs.current.cxttap(vis, e));
    vis.cy().on("dragfreeon", (e) => evs.current.dragfreeon(vis, e));
    vis.cy().on("dbltap", (e) => evs.current.dbltap(vis, e));
    vis.cy().on("render", (e) => evs.current.render(vis, e));
    vis.cy().on("select unselect", (e) => evs.current.select(vis, e));
  }, [vis, cytoscapeProps.style]);

  return vis.refs.cy;
};
