import * as actions from './actionTypes'

const initialState = () => ({
    page: "Database",
    cyLayout: {
        name: "cola",
        animate: true,
        infinite: false,
        avoidOverlap: false,
        randomize: false,
        maxSimulationTime: 2000,
        fit: false,
        centerGraph: true,
        convergenceThreshold: 0.02,
        nodeSpacing: 30,
        edgeLengthVal: 150,
        refresh: 10,
        edgeLength: (_e) => 150,
    },
    dbName: null,
    inspectedNode: null,
    stringProperties: [],
    nodeCache: {},
    edgeCache: {},
    selectedNodes: {},
    displayedProperty: null,
    hiddenNeighbors: { selectedNodeIds: [], edgeIds: [] },
    hiddenNodeIds: [],
});

const reducers = {
    page: (state = initialState().page, action) => {
        switch (action.type) {
            case actions.SET_PAGE:
                return action.payload.page;
            default:
                return state;
        }
    },

    cyLayout: (state = initialState().cyLayout, action) => {
        switch (action.type) {
            case actions.SET_CY_LAYOUT:
                return {
                    ...action.payload.cyLayout,
                    edgeLength: (_e) => action.payload.cyLayout.edgeLengthVal,
                }
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

    displayedProperty: (state = initialState().displayedProperty, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().displayedProperty;

            case actions.SELECT_DISPLAYED_PROPERTY:
                return action.payload.displayedProperty;

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

    hiddenNodeIds: (state = initialState().hiddenNodeIds, action) => {
        switch (action.type) {
            case actions.CLEAR:
                return initialState().hiddenNodeIds;
 
            case actions.HIDE_NODE:
                return [
                    ...state, action.payload.nodeId
                ].filter((e, i, arr) => arr.indexOf(e) === i)

            case actions.HIDE_NODES:
                return [
                    ...state, ...action.payload.nodeIds
                ].filter((e, i, arr) => arr.indexOf(e) === i)

            case actions.CLEAR_HIDDEN_NODES:
                return initialState().hiddenNodeIds;

            default:
                return state;
        }
    }
}

export default reducers;

