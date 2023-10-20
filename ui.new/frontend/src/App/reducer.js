import { combineReducers } from 'redux'
import * as actions from './actionTypes'
import * as visualizer from '../Visualizer/reducer';

const initialState = () => ({
    page: "Database",
    themeMode: "dark",
    visualizer: visualizer.initialState(),
    dbName: null,
    inspectedNode: null,
    stringProperties: [],
    nodeCache: {},
    edgeCache: {},
    selectedNodes: {},
    highlightedNodes: {},
    displayedNodeProperty: null,
    hiddenNeighbors: { selectedNodeIds: [], edgeIds: [] },
    filters: {
        showOnlyHomoSapiens: true,
        hidePublications: true,
        hideCompartments: true,
        hideSpecies: true,
        hideDatabaseReferences: true,
    }
});

const reducers = combineReducers({
    visualizer: visualizer.reducer,

    themeMode: (state = initialState().themeMode, action) => {
        switch (action.type) {
            case actions.SET_THEME_MODE: {
                return action.payload.mode;
            }
            default:
                return state;
        }
    },

    page: (state = initialState().page, action) => {
        switch (action.type) {
            case actions.SET_PAGE:
                return action.payload.page;
            default:
                return state;
        }
    },

    dbName: (state = initialState().dbName, action) => {
        switch (action.type) {
            case actions.SET_DB_NAME:
                return action.payload.dbName;

            default:
                return state;
        }
    },

    inspectedNode: (state = initialState().inspectedNode, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().inspectedNode;

            case actions.INSPECT_NODE:
                return action.payload.node;

            default:
                return state;
        }
    },

    selectedNodes: (state = initialState().selectedNodes, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().selectedNodes;

            case actions.SELECT_NODE:
                return {
                    ...state,
                    [action.payload.node.id]: { ...action.payload.node }
                };

            case actions.SELECT_NODES:
                return {
                    ...state,
                    ...Object.fromEntries(action.payload.nodes
                        .map(n => [n.id, { ...n }]))
                };

            case actions.UNSELECT_NODE:
                const newState = { ...state };
                delete newState[action.payload.node.id];
                return newState;

            case actions.CLEAR_SELECTED_NODES:
                return {};

            default:
                return state;
        }
    },

    //highlightedNodes: (state = initialState().highlightedNodes, action) => {
    //    switch (action.type) {
    //        case actions.CLEAR:
    //            return initialState().highlightedNodes;

    //        case actions.ADD_HIGHLIGHTED_NODES:
    //            return [
    //                ...state,
    //                ...action.payload.nodeIds
    //            ];

    //        default:
    //            return state;
    //    }
    //},

    nodeCache: (state = initialState().nodeCache, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().nodeCache;

            case actions.CACHE_NODE:
                return {
                    ...state,
                    [action.payload.node.id]: action.payload.node
                };

            case actions.CACHE_NODES:
                return {
                    ...state,
                    ...Object.fromEntries(action.payload.nodes.map(n => [n.id, n])),
                };

            case actions.CLEAR_CACHE:
                return {};

            default:
                return state;
        }
    },

    edgeCache: (state = initialState().edgeCache, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().edgeCache;

            case actions.CACHE_EDGES:
                return {
                    ...state,
                    ...Object.fromEntries(action.payload.edges.map(e => [e.id, e])),
                };

            default:
                return state;
        }
    },

    displayedNodeProperty: (state = initialState().displayedNodeProperty, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().displayedNodeProperty;

            case actions.SELECT_DISPLAYED_PROPERTY:
                return action.payload.displayedNodeProperty;

            default:
                return state;
        }
    },

    hiddenNeighbors: (state = initialState().hiddenNeighbors, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().hiddenNeighbors;

            case actions.HIDE_NEIGHBORS:
                return {
                    selectedNodeIds: [
                        ...state.selectedNodeIds, action.payload.nodeId
                    ],
                    edgeIds: [
                        ...state.edgeIds, ...action.payload.edgeIds
                    ].filter((e, i, arr) => arr.indexOf(e) === i)
                };

            case actions.SHOW_NEIGHBORS:
                return {
                    selectedNodeIds: state.selectedNodeIds.filter(
                        id => id !== action.payload.nodeId
                    ),
                    edgeIds: state.edgeIds.filter(
                        id => !action.payload.edgeIds.includes(id))
                };

            default:
                return state;
        }
    },
})

export default reducers;

