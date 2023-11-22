import React from 'react';
import * as cyEvents from "./events";

export const useVisualizerReferences = () => {
  const state = React.useRef();
  const events = React.useRef({
    onetap: (_vis, _e) => () => cyEvents.onetap(_vis, _e),
    cxttap: (vis, e) => cyEvents.cxttap(vis, e),
    dragfreeon: (vis, e) => cyEvents.dragfreeon(vis, e),
    dbltap: (vis, e) => cyEvents.dbltap(vis, e),
    render: (vis, e) => cyEvents.render(vis, e),
    select: (vis, e) => cyEvents.select(vis, e),
  });

  const callbacks = React.useRef();
  const triggers = React.useRef({
    elements: { core: {}, secondary: {} },
    selectedNodeIds: { core: {}, secondary: {} },
    filters: { core: {}, secondary: {} },
    layouts: { core: {}, secondary: {} },
    nodeLabel: { core: {}, secondary: {} },
    edgeLabel: { core: {}, secondary: {} },
    hiddenNodes: { core: {}, secondary: {} },
  });
  const dialogs = React.useRef({});
  const cy = React.useRef();

  const contextMenu = {
    ref: React.useRef(),
    data: React.useRef(),
    setData: React.useRef(),
  };

  return {
    state,
    events,
    dialogs,
    callbacks,
    triggers,
    cy,
    contextMenu,
  };
};


