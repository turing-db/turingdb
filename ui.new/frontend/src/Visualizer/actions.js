import * as actions from './actionTypes';

export const setFilters = filters => ({
    type: actions.SET_FILTERS,
    payload: { filters },
});

export const addLayout = (definition, nodeIds) => ({
    type: actions.ADD_LAYOUT,
    payload: { definition, nodeIds },
});

export const setDefaultLayout = nodeIds => ({
    type: actions.SET_DEFAULT_LAYOUT,
    payload: { nodeIds },
});

export const setCyLayout = cyLayout => ({
    type: actions.SET_CY_LAYOUT,
    payload: { cyLayout },
});

export const setEdgeLabel = edgeLabel => ({
    type: actions.SET_EDGE_LABEL,
    payload: { edgeLabel }
});

export const clearHiddenNodes = () => ({
    type: actions.CLEAR_HIDDEN_NODES,
    payload: {}
});

export const hideNode = nodeId => ({
    type: actions.HIDE_NODE,
    payload: { nodeId }
});

export const hideNodes = nodeIds => ({
    type: actions.HIDE_NODES,
    payload: { nodeIds }
});

export const showNodes = nodeIds => ({
    type: actions.SHOW_NODES,
    payload: { nodeIds }
});

export const setEdgeColorMode = (mode, elementIds = [], data = {}) => ({
    type: actions.SET_EDGE_COLOR_MODE,
    payload: { mode, elementIds, data }
})

export const setNodeColorMode = (mode, elementIds = [], data = {}) => ({
    type: actions.SET_NODE_COLOR_MODE,
    payload: { mode, elementIds, data }
})
