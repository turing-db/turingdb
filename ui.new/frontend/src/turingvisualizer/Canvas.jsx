// Core
import React from "react";
import cytoscape from "cytoscape";
import cola from "cytoscape-cola";

// Turing
import { style, useVisualizerContext, useCanvasTrigger } from ".";

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
  }, [vis, cytoscapeProps.style]);

  return vis.refs.cy;
};

const Canvas = () => {
  const vis = useVisualizerContext();
  const cy = useCytoscapeInstance();

  useCanvasTrigger({
    category: "elements",
    name: "core-setElements",
    core: true,
    callback: () => {
      if (!cy.current) return;

      const c = cy.current;
      const elements = vis.state().elements;
      const layouts = vis.state().layouts;
      const idsBefore = Object.fromEntries(
        c.elements().map((e) => [e.id(), true])
      );
      const mappedElements = Object.fromEntries(
        elements.map((e) => [e.data.id, e])
      );
      const toAdd = elements.filter((e) => idsBefore[e.data.id] === undefined);
      const toKeep = c.collection(
        Object.keys(idsBefore)
          .filter((id) => mappedElements[id])
          .map((id) => c.$id(id))
      );
      const toRm = c.collection(
        Object.keys(idsBefore)
          .filter((id) => !mappedElements[id])
          .map((id) => c.$id(id))
      );
      const patchedElements = {};

      c.batch(() => {
        toAdd.forEach((e) => c.add(e));
        toRm.forEach((e) => c.remove(e));

        // Patching exiting elements
        toKeep.forEach((e1) => {
          e1.unselect(); // Dismiss selection
          const e2 = mappedElements[e1.id()];
          const data1 = e1.data();
          const data2 = e2.data;

          // data keys
          const keys = ["type", "iconColor", "textColor", "lineColor", "label"];
          for (const key of keys) {
            if (data1[key] !== data2[key]) {
              e1.data(data2);
              patchedElements[e1.id()] = true;
              break;
            }
          }
        });
      });

      // If nodes were added or layouts were modified, apply layouts
      const nodesAdded = toAdd.length !== 0 || toRm.length !== 0;

      const runCallbacks = {
        cola: (it, lId, lDef) => {
          const eles = c.filter((e) => {
            const l1 = parseInt(layouts.mapping[e.id()] || 0);
            const l2 = parseInt(lId);
            return l1 === l2;
          });

          const rawAddedElements = Object.fromEntries(
            toAdd.map((n) => [n.id, n])
          );
          const addedElements = c.filter(
            (n) => rawAddedElements[n.id()] !== undefined
          );
          const cyPatchedElements = c.filter((n) => patchedElements[n.id()]);

          // If no elements to add or remove, it means the graph did not have
          // any element prior to this operation. Thus, we can center the camera
          const centerGraph = toKeep.length === 0 && toRm.length === 0;

          // Hack to start the animation from a decent initial guess:
          // - lock elements that did not change,
          // - run a first layout with the lock
          // - at the end of the run, unlock and re-run
          toKeep.lock();
          //cyPatchedElements.unlock();
          addedElements.style({
            opacity: 0.0,
          });

          const nextLayoutId = it.next().value;
          const nextLayout = layouts.definitions[nextLayoutId];
          const nextLayoutName = nextLayout?.name || "end";

          const stop = () => {
            toKeep.unlock();

            const stop = () =>
              runCallbacks[nextLayoutName](it, nextLayoutId, nextLayout);
            eles.layout({ ...lDef, centerGraph, stop }).run();
          };

          const animationTime = 400;
          const layout = eles.layout({
            ...lDef,
            maxSimulationTime: animationTime,
            avoidOverlap: false,
            refresh: 2,
            animate: true,
            centerGraph,
            handleDisconnected: false,
            stop,
          });

          addedElements.animate({
            style: { opacity: 1.0 },
            duration: animationTime + 1000,
            easing: "ease-in-out-expo",
          });

          layout.run();
        },

        preset: (it, lId, lDef) => {
          const eles = c.filter((e) => {
            const l1 = parseInt(layouts.mapping[e.id()] || 0);
            const l2 = parseInt(lId);
            return l1 === l2;
          });
          const nextLayoutId = it.next().value;
          const nextLayout = layouts.definitions[nextLayoutId];
          const nextLayoutName = nextLayout?.name || "end";
          const stop = () =>
            runCallbacks[nextLayoutName](it, nextLayoutId, nextLayout);

          eles.layout({ ...lDef, stop }).run();
        },

        random: (it, lId, lDef) => {
          const eles = c.filter((e) => {
            const l1 = parseInt(layouts.mapping[e.id()] || 0);
            const l2 = parseInt(lId);
            return l1 === l2;
          });
          const nextLayoutId = it.next().value;
          const nextLayout = layouts.definitions[nextLayoutId];
          const nextLayoutName = nextLayout?.name || "end";
          const stop = () =>
            runCallbacks[nextLayoutName](it, nextLayoutId, nextLayout);

          eles.layout({ ...lDef, stop }).run();
        },

        end: () => {},
      };

      if (nodesAdded || layouts.runRequested) {
        const firstLayout = layouts.definitions[0];
        const it = Object.keys(layouts.definitions)[Symbol.iterator]();
        const firstLayoutId = it.next().value;

        runCallbacks[firstLayout.name](it, firstLayoutId, firstLayout);
        vis.callbacks().requestLayoutRun(false);
      }
    },
  });

  return (
    <div
      id={vis.containerId}
      style={{
        position: "absolute",
        width: "100%",
        height: "100%",
      }}></div>
  );
};

export default Canvas;
