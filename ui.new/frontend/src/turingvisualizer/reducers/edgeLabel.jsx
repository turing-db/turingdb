import React from "react";

const SET_EDGE_LABEL = "SET_EDGE_LABEL";

export const edgeLabelInitialState = () => "Edge Type";

const useEdgeLabelReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return edgeLabelInitialState();

    case SET_EDGE_LABEL:
      return action.payload.edgeLabel;

    default:
      return state;
  }
};

const useEdgeLabel = () => {
  const [state, dispatch] = React.useReducer(
    useEdgeLabelReducer,
    edgeLabelInitialState()
  );

  const setEdgeLabel = (edgeLabel) =>
    dispatch({
      type: SET_EDGE_LABEL,
      payload: { edgeLabel },
    });

  return {
    edgeLabel: state,
    setEdgeLabel,
  };
};

export default useEdgeLabel;
