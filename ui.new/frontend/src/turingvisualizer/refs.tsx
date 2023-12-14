import React from "react";
import cytoscape from "cytoscape";
import * as cyEvents from "./events";
import {
  VisualizerState,
  VisualizerCallbacks,
  VisualizerEventHooks,
  CxtMenuData,
  VisualizerEvents,
  VisualizerTriggers,
  HookEventFunction,
} from "./types";

export const useVisualizerReferences = () => {
  const state = React.useRef<VisualizerState>();
  const events = React.useRef<VisualizerEvents>({
    onetap: (_vis, e) => cyEvents.onetap(_vis, e),
    cxttap: (vis, e) => cyEvents.cxttap(vis, e),
    dragfreeon: (vis, e) => cyEvents.dragfreeon(vis, e),
    dbltap: (vis, e) => cyEvents.dbltap(vis, e),
    render: (vis, e) => cyEvents.render(vis, e),
    select: (vis, e) => cyEvents.select(vis, e),
  });

  const callbacks = React.useRef<VisualizerCallbacks>();
  const triggers = React.useRef<VisualizerTriggers>({
    elements: { core: {}, secondary: {} },
    selectedNodeIds: { core: {}, secondary: {} },
    inspectedNode: { core: {}, secondary: {} },
    filters: { core: {}, secondary: {} },
    layouts: { core: {}, secondary: {} },
    nodeLabel: { core: {}, secondary: {} },
    edgeLabel: { core: {}, secondary: {} },
    hiddenNodes: { core: {}, secondary: {} },
  });
  const cy = React.useRef<cytoscape.Core>();

  const eventHooks = React.useRef<VisualizerEventHooks>({
    select: {},
  });

  const hookEvent = React.useRef<HookEventFunction>(
    (eventName: string, key: string, callback: () => void) => {
      eventHooks.current[eventName][key] = callback;
    }
  );

  const setContextMenuData = React.useRef<(d: CxtMenuData) => void>();
  const contextMenu = {
    ref: React.useRef<HTMLElement>(),
    data: React.useRef<any>(),
    setData: setContextMenuData,
  };

  return {
    state,
    events,
    callbacks,
    triggers,
    cy,
    contextMenu,
    eventHooks,
    hookEvent,
  };
};

export type VisualizerReferences = ReturnType<typeof useVisualizerReferences>
