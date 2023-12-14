// Core
import React from "react";
//import { renderToString } from "react-dom/server";
import cytoscape from "cytoscape";
import cola from "cytoscape-cola";
//import nodeHtmlLabel from "cytoscape-node-html-label";
//import { Icon } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "../context";
import style from "../style"

if (typeof cytoscape("core", "cola") === "undefined") {
  cytoscape.use(cola);
}
//if (typeof cytoscape("core", "nodeHtmlLabel") === "undefined") {
//  cytoscape.use(nodeHtmlLabel);
//}

export const useCytoscapeInstance = () => {
  const vis = useVisualizerContext();
  const cytoscapeProps = vis.state().cytoscapeProps;

  React.useEffect(() => {
    if (vis.cy()) return;

    vis.refs.cy.current = cytoscape({
      container: document.getElementById(vis.containerId),
      style: style,
    });
    //vis.cy().nodeHtmlLabel([
    //  {
    //    query: "node",
    //    halign: "center",
    //    valign: "center",
    //    halignBox: "right",
    //    valignBox: "top",
    //    tpl(data) {
    //      if (data.has_hidden_nodes) {
    //        return renderToString(
    //          <Icon className="primary-dark" icon="eye-off" />
    //        );
    //      }

    //      return;
    //    },
    //  },
    //]);

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
