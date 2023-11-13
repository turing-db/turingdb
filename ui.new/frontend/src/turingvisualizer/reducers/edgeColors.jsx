import React from "react";
import { COLOR_MODES } from "../constants";

const SET_EDGE_COLOR_MODE = "SET_EDGE_COLOR_MODE";

export const edgeColorsInitialState = () => ({
  colorSets: {
    0: { mode: COLOR_MODES.None, data: {} }, // Global
  },
  mapping: {
    // filled with 'edgeId: setId' entries
  },
  setCount: 1,
});

const useEdgeColorsReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return edgeColorsInitialState();

    case SET_EDGE_COLOR_MODE: {
      const newState = { ...state };
      const isGlobalAction = action.payload.elementIds.length === 0;
      const setId = isGlobalAction
        ? 0 // global set
        : newState.setCount; // new colorset;
      newState.setCount = isGlobalAction
        ? newState.setCount
        : newState.setCount + 1;
      const newSet = { mode: action.payload.mode, data: action.payload.data };

      action.payload.elementIds.forEach((id) => (newState.mapping[id] = setId));
      const presentSets = [...new Set([0, ...Object.values(newState.mapping)])];
      newState.colorSets = Object.fromEntries(
        presentSets.map((sid) => [sid, newState.colorSets[sid]])
      );
      newState.colorSets[setId] = newSet;

      return newState;
    }

    default:
      return state;
  }
};

const useEdgeColors = () => {
  const [state, dispatch] = React.useReducer(
    useEdgeColorsReducer,
    edgeColorsInitialState()
  );

  const setEdgeColorMode = React.useCallback(
    (mode, elementIds = [], data = {}) =>
      dispatch({
        type: SET_EDGE_COLOR_MODE,
        payload: { mode, elementIds, data },
      }),
    []
  );

  return {
    edgeColors: state,
    setEdgeColorMode,
  };
};

export default useEdgeColors;
