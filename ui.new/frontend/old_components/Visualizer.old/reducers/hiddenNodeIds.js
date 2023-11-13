import React from 'react';

const HIDE_NODE = "HIDE_NODE";
const HIDE_NODES = "HIDE_NODES";
const SHOW_NODES = "SHOW_NODES";
const CLEAR_HIDDEN_NODES = "CLEAR_HIDDEN_NODES";

export const hiddenNodeIdsInitialState = () => [];

const useHiddenNodeIdsReducer = (state, action) => {
    switch (action.type) {
        case "CLEAR":
            return hiddenNodeIdsInitialState();

        case HIDE_NODE:
            return [
                ...state,
                action.payload.nodeId
            ].filter((e, i, arr) => arr.indexOf(e) === i);

        case HIDE_NODES:
            return [
                ...state,
                ...action.payload.nodeIds
            ].filter((e, i, arr) => arr.indexOf(e) === i);

        case SHOW_NODES:
            return state.filter(id => !action.payload.nodeIds.includes(parseInt(id)));

        case CLEAR_HIDDEN_NODES: {
            return hiddenNodeIdsInitialState();
        }

        default:
            return state;
    }
};

const useHiddenNodeIds = () => {
    const [state, dispatch] = React.useReducer(useHiddenNodeIdsReducer, hiddenNodeIdsInitialState());

    const hideNode = React.useCallback((nodeId) => dispatch({
        type: HIDE_NODE,
        payload: { nodeId },
    }), []);

    const hideNodes = React.useCallback((nodeIds) => dispatch({
        type: HIDE_NODES,
        payload: { nodeIds },
    }), []);

    const showNodes = React.useCallback((nodeIds) => dispatch({
        type: SHOW_NODES,
        payload: { nodeIds },
    }), []);

    const clearHiddenNodes = React.useCallback(() => dispatch({
        type: CLEAR_HIDDEN_NODES,
        payload: { },
    }), []);

    return {
        hiddenNodeIds: state,
        hideNode,
        hideNodes,
        showNodes,
        clearHiddenNodes,
    };
}

export default useHiddenNodeIds;
