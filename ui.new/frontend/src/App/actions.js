import * as actions from './actionTypes';

export const setPage = page => ({
    type: actions.SET_PAGE,
    payload: { page },
});

export const setCyLayout = cyLayout => ({
    type: actions.SET_CY_LAYOUT,
    payload: { cyLayout },
})

export const setDbName = dbName => ({
    type: actions.SET_DB_NAME,
    payload: { dbName },
});

export const inspectNode = node => ({
    type: actions.INSPECT_NODE,
    payload: { node },
});

export const cacheNode = (node) => ({
    type: actions.CACHE_NODE,
    payload: { node },
});

export const cacheNodes = (nodes) => ({
    type: actions.CACHE_NODES,
    payload: { nodes },
});

export const cacheEdges = (edges) => ({
    type: actions.CACHE_EDGES,
    payload: { edges },
});

export const selectNode = (node) => ({
    type: actions.SELECT_NODE,
    payload: { node },
});

export const selectNodes = (nodes) => ({
    type: actions.SELECT_NODES,
    payload: { nodes },
});

export const unselectNode = (node) => ({
    type: actions.UNSELECT_NODE,
    payload: { node },
});

export const clearSelectedNodes = () => ({
    type: actions.CLEAR_SELECTED_NODES,
    payload: {},
});
