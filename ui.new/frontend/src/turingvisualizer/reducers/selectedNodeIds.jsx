import React from "react";

const SET_SELECTED_NODE_IDS = "SET_SELECTED_NODE_IDS";

export const selectedNodeIdsInitialState = () => [];

const useSelectedNodeIdsReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return selectedNodeIdsInitialState();

    case SET_SELECTED_NODE_IDS: {
      return [...action.payload.selectedNodeIds];
    }

    default:
      return state;
  }
};

const useSelectedNodeIds = () => {
  const [state, dispatch] = React.useReducer(
    useSelectedNodeIdsReducer,
    selectedNodeIdsInitialState()
  );

  const setSelectedNodeIds = React.useCallback(
    (selectedNodeIds) =>
      dispatch({
        type: SET_SELECTED_NODE_IDS,
        payload: { selectedNodeIds },
      }),
    []
  );

  return {
    selectedNodeIds: state,
    setSelectedNodeIds,
  };
};

export default useSelectedNodeIds;
