import * as actions from './actionTypes'
import { EDGE_COLOR_MODES } from './constants'

export const initialState = () => ({
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
        positions: {},
        nodeSpacing: 30,
        edgeLengthVal: 150,
        refresh: 10,
        edgeLength: (_e) => 150,
        lineCount: 0,
    },
    layouts: {
        definitions: {}, // Maps the layout definition to a unique id
        mapping: {}, // Maps the node ids to one of the layout ids
        layoutCount: 0,
    },
    filters: {
        showOnlyHomoSapiens: true,
        hidePublications: true,
        hideCompartments: true,
        hideSpecies: true,
        hideDatabaseReferences: true,
    },
    edgeLabel: "EdgeType",
    hiddenNodeIds: [],

    edgeColorMode: {
        mode: EDGE_COLOR_MODES.None,
        data: {},
    }
})

export const reducer = (state = initialState(), action) => {
    switch (action.type) {
        case actions.CLEAR:
            return initialState();

        case actions.SET_CY_LAYOUT: {
            return {
                ...state,
                cyLayout: {
                    ...action.payload.cyLayout,
                    edgeLength: (_e) => action.payload.cyLayout.edgeLengthVal,
                }
            };
        }

        case actions.ADD_LAYOUT:
            return {
                ...state,
                layouts: {
                    definitions: {
                        ...state.layouts.definitions,
                        [state.layouts.layoutCount]: action.payload.definition,
                    },
                    mapping: {
                        ...state.layouts.mapping,
                        ...Object.fromEntries(action.payload.nodeIds.map(id =>
                            [id, state.layouts.layoutCount]
                        ))
                    },
                    layoutCount: state.layouts.layoutCount + 1,
                }
            };

        case actions.SET_DEFAULT_LAYOUT: {
            const newState = {
                ...state,
                layouts: {
                    definitions: { ...state.layouts.definitions },
                    mapping: { ...state.layouts.mapping },
                    layoutCount: state.layouts.layoutCount,
                }
            };

            for (const id of action.payload.nodeIds) {
                const layoutId = newState.layouts.mapping[id];
                if (layoutId === undefined) continue;

                const layout = newState.layouts.definitions[layoutId];
                delete newState.layouts.mapping[id];
                delete newState.layouts.definitions[layoutId].positions[id]

                if (Object.keys(layout.positions).length === 0) {
                    delete newState.layouts.definitions[layoutId];
                }
            }

            return newState;
        }

        case actions.SET_FILTERS: {
            return {
                ...state,
                filters: {
                    ...state.filters,
                    ...action.payload.filters,
                }
            };
        }

        case actions.SET_EDGE_LABEL:
            return {
                ...state,
                edgeLabel: action.payload.edgeLabel
            };

        case actions.HIDE_NODE:
            return {
                ...state,
                hiddenNodeIds: [
                    ...state.hiddenNodeIds,
                    action.payload.nodeId
                ].filter((e, i, arr) => arr.indexOf(e) === i)
            };

        case actions.HIDE_NODES:
            return {
                ...state,
                hiddenNodeIds: [
                    ...state.hiddenNodeIds,
                    ...action.payload.nodeIds
                ].filter((e, i, arr) => arr.indexOf(e) === i)
            };

        case actions.CLEAR_HIDDEN_NODES: {
            return {
                ...state,
                hiddenNodeIds: initialState().hiddenNodeIds
            };
        }

        case actions.SET_EDGE_COLOR_MODE: {
            return {
                ...state,
                edgeColorMode: {
                    mode: action.payload.mode,
                    data: action.payload.data
                }
            };
        }
        default:
            return state;
    }
}
