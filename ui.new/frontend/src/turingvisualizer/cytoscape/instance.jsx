// Core
import React from "react";
//import { renderToString } from "react-dom/server";
import cytoscape from "cytoscape";
import cola from "cytoscape-cola";
//import nodeHtmlLabel from "cytoscape-node-html-label";
//import { Icon } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "../context";
import style from "../style";
import { getFragment } from "./tools";

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
    const container = document.getElementById(vis.containerId);

    vis.refs.cy.current = cytoscape({
      container: container,
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
    vis.cy().on("mousedown", (e) => {
      vis.cy().boxSelectionEnabled(true);
      if (!e.target?.group) return;
      if (e.target.group() !== "nodes") return;
      if (e.originalEvent.shiftKey || e.originalEvent.ctrlKey) {
        vis.cy().boxSelectionEnabled(false);
        return;
      }
    });

    vis.cy().on("grabon", (e) => {
      if (!e.target?.group) return;
      if (e.target.group() !== "nodes") return;
      const n = e.target;
      const p = n.position();
      n.scratch("turing", {
        previousPosition: { x: p.x, y: p.y },
        shift: { x: 0, y: 0 },
        shiftKey: e.originalEvent.shiftKey,
        ctrlKey: e.originalEvent.ctrlKey,
      });
    });

    vis.cy().on("drag", (e) => {
      if (!e.target?.group) return;
      if (e.target.group() !== "nodes") return;
      const n = e.target;
      const scratch = { ...n.scratch("turing") };

      if (scratch.shiftKey || scratch.ctrlKey) {
        const p = n.position();

        scratch.shift.x = p.x - scratch.previousPosition.x;
        scratch.shift.y = p.y - scratch.previousPosition.y;

        // if shift key, whole fragment. else if ctrl key, only unique neighbors
        const frag = scratch.shiftKey
          ? getFragment(vis.cy(), n.id()).difference(n).filter(n => !n.selected())
          : vis
              .cy()
              .$id(n.id())
              .neighborhood()
              .filter((e) => {
                if (e.group === "edges") return true;
                return e.connectedEdges().length <= 1;
              })
              .filter((e) => !e.selected());

        frag.shift(scratch.shift);

        scratch.shift = { x: 0, y: 0};
        scratch.previousPosition.x = p.x;
        scratch.previousPosition.y = p.y;
        n.scratch("turing", scratch);
      }
    });

    container.setAttribute("tabindex", 0);
    container.addEventListener("mousemove", () => {
      container.focus();
      return false;
    });
    container.addEventListener("keydown", (e) => {
      if (e.key === "a" && (e.ctrlKey || e.metaKey)) {
        vis.cy().elements().select();
        e.preventDefault();
        return true;
      }
    });
    container.addEventListener("keydown", (e) => {
      if (e.key === "f" && (e.ctrlKey || e.metaKey)) {
        vis.searchNodesDialog.open();
        e.preventDefault();
        return true;
      }
    });
  }, [vis, cytoscapeProps.style]);

  return vis.refs.cy;
};
