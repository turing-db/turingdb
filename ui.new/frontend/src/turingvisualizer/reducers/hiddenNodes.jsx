import React from "react";

const HIDE_NODE = "HIDE_NODE";
const HIDE_NODES = "HIDE_NODES";
const SHOW_NODES = "SHOW_NODES";

export const hiddenNodesInitialState = () => ({});

const useHiddenNodesReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return hiddenNodesInitialState();

    case HIDE_NODE:
      return {
        ...state,
        [action.payload.node.turing_id]: action.payload.node,
      };

    case HIDE_NODES:
      return {
        ...state,
        ...action.payload.nodes,
      };

    case SHOW_NODES: {
      action.payload.nodeIds.forEach((id) => {
        if (state[id] !== undefined) delete state[id];
      });
      return {
        ...state,
      };
    }

    default:
      return state;
  }
};

const useHiddenNodes = () => {
  const [state, dispatch] = React.useReducer(
    useHiddenNodesReducer,
    hiddenNodesInitialState()
  );

  const hideNode = React.useCallback(
    (node) =>
      dispatch({
        type: HIDE_NODE,
        payload: { node },
      }),
    []
  );

  const hideNodes = React.useCallback(
    (nodes) =>
      dispatch({
        type: HIDE_NODES,
        payload: { nodes },
      }),
    []
  );

  const showNodes = React.useCallback(
    (nodeIds) =>
      dispatch({
        type: SHOW_NODES,
        payload: { nodeIds },
      }),
    []
  );

  return {
    hiddenNodes: state,
    hideNode,
    hideNodes,
    showNodes,
  };
};

export default useHiddenNodes;
