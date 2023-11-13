import React from "react";

const SET_NODE_LABEL = "SET_NODE_LABEL";

export const nodeLabelInitialState = () => "Node Type";

const useNodeLabelReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return nodeLabelInitialState();

    case SET_NODE_LABEL:
      return action.payload.nodeLabel;

    default:
      return state;
  }
};

const useNodeLabel = () => {
  const [state, dispatch] = React.useReducer(
    useNodeLabelReducer,
    nodeLabelInitialState()
  );

  const setNodeLabel = (nodeLabel) =>
    dispatch({
      type: SET_NODE_LABEL,
      payload: { nodeLabel },
    });

  return {
    nodeLabel: state,
    setNodeLabel,
  };
};

export default useNodeLabel;
