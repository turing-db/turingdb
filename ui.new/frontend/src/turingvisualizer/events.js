export const onetap = (_vis, _e) => {};

export const cxttap = (vis, e) => {
  const target = e.target;
  const newEvent = new e.originalEvent.constructor(
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
        .forEach((e) => e.unselect());
      target.select();
    }
    vis.contextMenuSetData({ group: "nodes", data: target.data() });
  } else {
    // If clicked a edge that is not highlighted, make it highlighted
    if (!target.selected()) {
      vis
        .cy()
        .elements()
        .forEach((e) => e.unselect());
      target.select();
    }
    vis.contextMenuSetData({ group: "edges", data: target.data() });
  }

  vis.contextMenu().dispatchEvent(newEvent);
};

export const dragfreeon = (vis, _e) => {
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
      return layout.name === "preset";
    })
    .forEach((n) => {
      const layoutId = mapping[n.data().id];
      positions[layoutId][n.data().id] = n.position();
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

export const dbltap = (vis, e) => {
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
    if (n.neighborNodeIds.includes(node.turing_id)) {
      vis.callbacks().showNodes([hiddenNodeId]);
    }
  });

  vis
    .callbacks()
    .setSelectedNodeIds([...vis.state().selectedNodeIds, node.turing_id]);
};

export const select = (vis) => {
  //const unselected = vis.cy().$(":unselected");
  //const selected = vis.cy().$(":selected");

  //unselected.clearQueue();
  //selected.clearQueue();
  //unselected.stop();
  //selected.stop();

  //if (selected.length !== 0) {
  //  selected.animate({
  //    style: { opacity: 1.0 },
  //  });
  //  unselected.animate({
  //    style: { opacity: 1.0 },
  //  });
  //} else {
  //  unselected.animate({
  //    style: { opacity: 1.0 },
  //  });
  //}
};

export const render = (vis, e) => {};
