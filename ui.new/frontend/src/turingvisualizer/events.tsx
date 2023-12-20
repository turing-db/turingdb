import { getFitViewport } from "./cytoscape/tools";
import cytoscape from "cytoscape";
import { Visualizer, CanvasEvent } from "./types";

export const onetap: CanvasEvent = (
  vis: Visualizer,
  e: cytoscape.EventObject
) => {
  if (e.target.group === undefined) return;
  if (e.target.group() !== "nodes") return;
  vis.callbacks().setInspectedNode(e.target.data());
};

export const cxttap: CanvasEvent = (vis, e) => {
  const target = e.target;
  type EventConstructor = new (name: string, e: MouseEvent) => MouseEvent;
  const constructEvent = e.originalEvent.constructor as EventConstructor;

  const newEvent: MouseEvent = new constructEvent(
    "contextmenu",
    e.originalEvent
  );

  // Clicked on the background
  if (!target.group) {
    vis.contextMenuSetData({ group: "background", data: target.elements() });
  }

  // Clicked on a node
  else if (target.group() === "nodes") {
    // If clicked a node that is not highlighted, make it highlighted
    if (!target.selected()) {
      vis
        .cy()
        .elements()
        .forEach((e) => {
          e.unselect();
        });
      target.select();
    }
    vis.contextMenuSetData({ group: "nodes", data: target.data() });
  } else {
    // If clicked a edge that is not highlighted, make it highlighted
    if (!target.selected()) {
      vis
        .cy()
        .elements()
        .forEach((e) => {
          e.unselect();
        });
      target.select();
    }
    vis.contextMenuSetData({ group: "edges", data: target.data() });
  }

  vis.contextMenu().dispatchEvent(newEvent);
};

export const dragfreeon: CanvasEvent = (vis, _e) => {
  const layouts = vis.state().layouts;
  const mapping = layouts.mapping;
  const positions = Object.fromEntries(
    Object.entries(layouts.definitions)
      .filter(([_lId, l]) => l.name === "preset")
      .map(([lId, l]) => [lId, l.positions])
  );

  vis
    .cy()
    .nodes()
    .filter((n) => n.selected())
    .filter((n) => {
      const layoutId = mapping[n.data().id] || 0;
      const layout = layouts.definitions[layoutId];
      if (!layout) return false;

      return layout.name === "preset";
    })
    .forEach((n) => {
      const layoutId = mapping[n.data().id];
      const id: string = n.data().id;
      positions[layoutId]![id] = n.position();
    });

  Object.entries(positions).forEach(([lId, p]) => {
    vis.callbacks().updateLayout(lId, {
      positions: {
        ...vis.state().layouts.definitions[lId].positions,
        ...p,
      },
    });
  });
};

export const dbltap: CanvasEvent = (vis, e) => {
  const target = e.target;

  if (!target.group) {
    vis.cy().fit();
    return;
  }

  const node = target.data();
  if (target.group() !== "nodes") {
    vis.cy().fit();
    return;
  }

  Object.entries(vis.state().hiddenNodes).forEach(([hiddenNodeId, n]) => {
    if (n.neighborNodeIds?.includes(node.turing_id)) {
      vis.callbacks().showNodes([hiddenNodeId]);
    }
  });

  vis
    .callbacks()
    .setSelectedNodeIds([...vis.state().selectedNodeIds, node.turing_id]);

  if (vis.state().layouts.centerOnDoubleClicked) {
    vis.cy().animate({
      center: {
        eles: target,
      },
      duration: 700,
      easing: "ease",
    });
  }
};

export const select: CanvasEvent = (vis) => {
  const unselected = vis.cy().$(":unselected");
  const selected = vis.cy().$(":selected");

  if (unselected.length !== 0) {
    unselected.clearQueue();
    selected.clearQueue();
    unselected.stop();
    selected.stop();
    unselected.animate({
      style: {
        opacity: 1.0,
      },
      duration: 200,
      easing: "ease-in",
    });
  }

  Object.values(vis.eventHooks()["select"]!).forEach((fn) => fn());
};

export const render: CanvasEvent = (vis) => {
  if (vis.cy().elements().length === 0) return;
  const viewport = getFitViewport(vis.cy(), vis.cy().elements(), 100);
  const zoom = viewport.zoom!;
  vis.cy().minZoom(zoom * 0.5);
  vis.cy().maxZoom(6);
};
