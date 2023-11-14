import React from "react";
import * as cyEvents from "./events";

const useVisualizerReferences = () => {
  const state = React.useRef();
  const events = React.useRef({
    onetap: (_vis, _e) => () => {},
    cxttap: (vis, e) => cyEvents.cxttap(vis, e),
    dragfreeon: (vis, e) => cyEvents.dragfreeon(vis, e),
    dbltap: (vis, e) => cyEvents.dbltap(vis, e),
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
  const cy = React.useRef();

  const contextMenu = {
    ref: React.useRef(),
    data: React.useRef(),
    setData: React.useRef(),
  };

  return {
    state,
    events,
    callbacks,
    triggers,
    cy,
    contextMenu,
  };
};

export const VisualizerContext = React.createContext();
export const VisualizerContextProvider = ({
  children,
  canvasTheme,
  themeMode,
  dbName,
  containerId,
  devMode = false,
}) => {
  const refs = useVisualizerReferences();

  return (
    <VisualizerContext.Provider
      value={{
        refs,
        themeMode: themeMode,
        canvasTheme: canvasTheme,
        dbName: dbName,
        devMode,
        containerId,
        state: () => refs.state.current,
        events: () => refs.events.current,
        callbacks: () => refs.callbacks.current,
        triggers: () => refs.triggers.current,
        cy: () => refs.cy.current,
        contextMenu: () => refs.contextMenu.ref.current,
        contextMenuData: () => refs.contextMenu.data.current,
        contextMenuSetData: (data) => refs.contextMenu.setData.current(data),
      }}>
      {children}
    </VisualizerContext.Provider>
  );
};

export const useVisualizerContext = () => React.useContext(VisualizerContext);

export { default as Canvas } from "./Canvas";
export { default as Visualizer } from "./Visualizer";
export { default as TuringContextMenu } from "./TuringContextMenu";
export { default as TestApp } from "./TestApp";
export { default as style } from "./cyStyle";
export { ContextMenu, useContextMenuData } from "./ContextMenu";
export { useCanvasTrigger } from "./tools";
