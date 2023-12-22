import React from "react";

export const SET_DEFAULT_CY_LAYOUT = "SET_CY_LAYOUT";
export const ADD_LAYOUT = "ADD_LAYOUT";
export const UPDATE_LAYOUT = "UPDATE_LAYOUT";
export const UPDATE_LAYOUTS = "UPDATE_LAYOUTS";
export const RESET_LAYOUT = "RESET_LAYOUT";
export const REQUEST_RUN = "REQUEST_RUN";
export const REQUEST_FIT = "REQUEST_FIT";
export const CENTER_ON_DOUBLE_CLICKED = "CENTER_ON_DOUBLE_CLICKED";

export const lockBehaviours = {
  ALL_SELECTED: 0,
  INTERACTED: 1,
  DO_NOT_LOCK: 2,
};

export const INIT_EDGE_VAL = 130;

const getEdgeLengthFn = (v) => (edge) => {
  const connectingMultiplier = (edge.data().type === "connecting") * 0.2 + 1;
  const sourceExtent = edge.source().connectedEdges().length - 1;
  const targetExtent = edge.target().connectedEdges().length - 1;
  const ratio = (1 / (Math.max(sourceExtent, targetExtent) + 1)) * 0.8;
  return (v + v * ratio) * connectingMultiplier;
};

const initialColaLayout = () => ({
  name: "cola",
  animate: true,
  handleDisconnected: false,
  infinite: false,
  randomize: false,
  avoidOverlap: true,
  maxSimulationTime: 1000,
  fit: false,
  centerGraph: false,
  convergenceThreshold: 0.005,
  nodeSpacing: 5,
  nodeDimensionsIncludeLabels: false,
  edgeLengthVal: INIT_EDGE_VAL,
  refresh: 1,
  edgeLength: getEdgeLengthFn(INIT_EDGE_VAL),
  lineCount: 0,
  lockBehaviour: lockBehaviours.INTERACTED,
});

export const layoutsInitialState = () => ({
  definitions: {
    0: initialColaLayout(),
  }, // Maps the layout definition to a unique id
  mapping: {}, // Maps the node ids to one of the layout ids
  layoutCount: 1,
  runRequested: false,
  fitRequested: false,
  centerOnDoubleClicked: false,
});

const useLayoutsReducer = (state, action) => {
  switch (action.type) {
    case "CLEAR":
      return layoutsInitialState();

    case SET_DEFAULT_CY_LAYOUT: {
      return {
        ...state,
        definitions: {
          ...state.definitions,
          0: {
            ...action.payload.cyLayout,
            edgeLength: getEdgeLengthFn(action.payload.cyLayout.edgeLengthVal),
          },
        },
        runRequested: true,
      };
    }

    case ADD_LAYOUT:
      return {
        definitions: {
          ...state.definitions,
          [state.layoutCount]: action.payload.definition,
        },
        mapping: {
          ...state.mapping,
          ...Object.fromEntries(
            action.payload.nodeIds.map((id) => [id, state.layoutCount])
          ),
        },
        layoutCount: state.layoutCount + 1,
        runRequested: true,
      };

    case UPDATE_LAYOUT:
      return {
        ...state,
        definitions: {
          ...state.definitions,
          [action.payload.layoutId]: {
            ...state.definitions[action.payload.layoutId],
            ...action.payload.patch,
          },
        },
      };

    case UPDATE_LAYOUTS: {
      const newState = { ...state };
      newState.definitions = {
        ...newState.definitions,
        ...action.payload.layoutIds.map((lId) => {
          const patch = action.payload.patches[lId];
          return {
            ...newState.definitions[lId],
            ...patch,
          };
        }),
      };
      return newState;
    }

    case RESET_LAYOUT: {
      const newState = {
        definitions: { ...state.definitions },
        mapping: { ...state.mapping },
        layoutCount: state.layoutCount,
        runRequested: true,
      };

      for (const id of action.payload.nodeIds) {
        const layoutId = newState.mapping[id];
        if (layoutId === undefined || layoutId === 0) continue;

        const layout = newState.definitions[layoutId];
        delete newState.mapping[id];
        delete newState.definitions[layoutId].positions[id];

        if (Object.keys(layout.positions).length === 0) {
          delete newState.definitions[layoutId];
        }
      }

      return newState;
    }

    case REQUEST_RUN: {
      return {
        ...state,
        runRequested: action.payload.request,
      };
    }

    case REQUEST_FIT: {
      return {
        ...state,
        fitRequested: action.payload.request,
      }
    }

    case CENTER_ON_DOUBLE_CLICKED: {
      return {
        ...state,
        centerOnDoubleClicked: action.payload.value,
      }
    }

    default:
      return state;
  }
};

const useLayouts = () => {
  const [state, dispatch] = React.useReducer(
    useLayoutsReducer,
    layoutsInitialState()
  );
  const setDefaultCyLayout = React.useCallback(
    (cyLayout) =>
      dispatch({
        type: SET_DEFAULT_CY_LAYOUT,
        payload: { cyLayout },
      }),
    []
  );

  const addLayout = React.useCallback(
    (definition, nodeIds) =>
      dispatch({
        type: ADD_LAYOUT,
        payload: { definition, nodeIds },
      }),
    []
  );

  const updateLayout = React.useCallback(
    (layoutId, patch) =>
      dispatch({
        type: UPDATE_LAYOUT,
        payload: { layoutId, patch },
      }),
    []
  );

  const updateLayouts = React.useCallback(
    (layoutIds, patchs) =>
      dispatch({
        type: UPDATE_LAYOUTS,
        payload: { layoutIds, patchs },
      }),
    []
  );

  const resetLayout = React.useCallback(
    (nodeIds) =>
      dispatch({
        type: RESET_LAYOUT,
        payload: { nodeIds },
      }),
    []
  );

  const requestLayoutRun = React.useCallback(
    (request) =>
      dispatch({
        type: REQUEST_RUN,
        payload: { request },
      }),
    []
  );

  const requestLayoutFit = React.useCallback(
    (request) =>
      dispatch({
        type: REQUEST_FIT,
        payload: { request },
      }),
    []
  );

  const centerOnDoubleClicked = React.useCallback(
    (value) =>
      dispatch({
        type: CENTER_ON_DOUBLE_CLICKED,
        payload: { value },
      }),
    []
  );

  return {
    layouts: state,
    setDefaultCyLayout,
    addLayout,
    updateLayout,
    resetLayout,
    requestLayoutRun,
    requestLayoutFit,
    centerOnDoubleClicked,
  };
};

export default useLayouts;
