import React from "react";

const SET_INSPECTED_NODE = "SET_INSPECTED_NODE";

export const inspectedNodeInitialState = () => {};

const useInspectedNodeReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return inspectedNodeInitialState();

    case SET_INSPECTED_NODE: {
      return {...action.payload.inspectedNode};
    }

    default:
      return state;
  }
};

const useInspectedNode = () => {
  const [state, dispatch] = React.useReducer(
    useInspectedNodeReducer,
    inspectedNodeInitialState()
  );

  const setInspectedNode = React.useCallback(
    (inspectedNode) =>
      dispatch({
        type: SET_INSPECTED_NODE,
        payload: { inspectedNode },
      }),
    []
  );

  return {
    inspectedNode: state,
    setInspectedNode,
  };
};

export default useInspectedNode;
