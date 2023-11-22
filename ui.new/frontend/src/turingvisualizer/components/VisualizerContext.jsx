import React from 'react';
import { useVisualizerReferences } from '../refs';

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
        dialogs: () => refs.dialogs.current,
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
