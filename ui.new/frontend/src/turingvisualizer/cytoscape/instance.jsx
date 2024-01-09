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
    vis.cy().ready(() => vis.cy().scratch("turing", {}))
    vis.cy().on("onetap", (e) => evs.current.onetap(vis, e));
    vis.cy().on("cxttap", (e) => evs.current.cxttap(vis, e));
    vis.cy().on("dragfreeon", (e) => evs.current.dragfreeon(vis, e));
    vis.cy().on("dbltap", (e) => evs.current.dbltap(vis, e));
    vis.cy().on("render", (e) => evs.current.render(vis, e));
    vis.cy().on("select unselect", (e) => evs.current.select(vis, e));
    vis.cy().on("mousemove", (e) => {
      const globalScratch = vis.cy().scratch("turing");
      globalScratch.shifting = true;
    });

    vis.cy().on("mousedown", (e) => {
      vis.cy().boxSelectionEnabled(true);
      if (!e.target?.group) return;
      if (e.target.group() !== "nodes") return;
      if (
        e.originalEvent.shiftKey ||
        e.originalEvent.ctrlKey ||
        e.originalEvent.metaKey
      ) {
        vis.cy().boxSelectionEnabled(false);
        return;
      }
    });

    vis.cy().on("grab", (e) => {
      if (!e.target?.group) return;
      if (e.target.group() !== "nodes") return;
      const n = e.target;
      const p = n.position();

      n.scratch("turing", {
        previousPosition: { x: p.x, y: p.y },
        shiftKey: e.originalEvent.shiftKey,
        ctrlKey: e.originalEvent.ctrlKey || e.originalEvent.metaKey,
      });
    });

    vis.cy().on("drag", (e) => {
      const globalScratch = vis.cy().scratch("turing");

      if (!e.target?.group) return;
      if (e.target.group() !== "nodes") return;
      if (!globalScratch.shifting) return;

      const baseN = e.target;
      const scratch = { ...baseN.scratch("turing") };
      const p = baseN.position();

      globalScratch.shift = {
        x: p.x - scratch.previousPosition.x,
        y: p.y - scratch.previousPosition.y,
      };

      const fragments = [];
      const selectedElements = vis.cy().$(":selected");
      const baseElements = selectedElements.union(baseN);

      if (scratch.shiftKey) {
        for (const n of baseElements) {
          fragments.push(getFragment(vis.cy(), n.id()));
        }
      } else if (scratch.ctrlKey) {
        for (const n of baseElements) {
          fragments.push(n);
          fragments.push(
            vis
              .cy()
              .$id(n.id())
              .neighborhood()
              .filter((el) => {
                if (el.group() === "edges") return false;
                const connectedNodeIds = new Set([
                  ...el
                    .connectedEdges()
                    .map((edge) =>
                      edge.target().id() === el.id()
                        ? edge.source().id()
                        : edge.target().id()
                    ),
                ]);
                return connectedNodeIds.size <= 1;
              })
          );
        }
      }

      if (fragments.length === 0) return;

      const frag = vis.cy().collection();
      fragments.forEach((f) => frag.merge(f));

      if (selectedElements.contains(baseN)) {
        frag.unmerge(baseElements);
      } else {
        frag.unmerge(baseN);
      }

      frag.shift(globalScratch.shift);
      scratch.previousPosition = { ...baseN.position() };
      baseN.scratch("turing", scratch);
      globalScratch.shifting = false;
    });

    container.setAttribute("tabindex", 0);
    document.addEventListener("keydown", (e) => {
      if (!vis.cy()) return;

      if (e.key === "a" && (e.ctrlKey || e.metaKey)) {
        vis.cy().elements().select();
        e.preventDefault();
        return true;
      }
    });
    document.addEventListener("keydown", (e) => {
      if (!vis.cy()) return;

      if (e.key === "f" && (e.ctrlKey || e.metaKey)) {
        vis.searchNodesDialog.open();
        e.preventDefault();
        return true;
      }
    });
  }, [vis, cytoscapeProps.style]);

  return vis.refs.cy;
};
