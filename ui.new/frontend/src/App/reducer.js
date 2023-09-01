import * as actions from './actionTypes'

const initialState = () => ({
    page: "Database",
    cyLayout: {
        name: "cola",
        animate: true,
        infinite: false,
        avoidOverlap: false,
        randomize: false,
        fit: false,
        centerGraph: true,
        convergenceThreshold: 0.1,
        nodeSpacing: 30,
        edgeLengthVal: 50,
    },
    dbName: null,
    inspectedNode: null,
    stringProperties: [],
    nodeCache: {},
    edgeCache: {},
    selectedNodes: {},
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
                return action.payload.cyLayout;
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
            case actions.INSPECT_NODE:
                return action.payload.node;

            default:
                return state;
        }
    },

    selectedNodes: (state = initialState().selectedNodes, action) => {
        switch (action.type) {
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

            default:
                return state;
        }
    },

    edgeCache: (state = initialState().edgeCache, action) => {
        switch (action.type) {
            case actions.CACHE_EDGES:
                return {
                    ...state,
                    ...Object.fromEntries(action.payload.edges.map(e => [e.id, e])),
                };

            default:
                return state;
        }
    }
}

export default reducers;
