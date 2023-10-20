import * as actions from './actionTypes'
import { COLOR_MODES } from './constants'

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

    nodeColors: {
        colorSets: {
            0: { mode: COLOR_MODES.None, data: {} } // Global
        },
        mapping: {
            // filled with 'edgeId: setId' entries
        },
        setCount: 1,
    },

    edgeColors: {
        colorSets: {
            0: { mode: COLOR_MODES.None, data: {} } // Global
        },
        mapping: {
            // filled with 'edgeId: setId' entries
        },
        setCount: 1,
    },

    edgeColorMode: {
        mode: COLOR_MODES.None,
        data: {},
    },

    nodeColorMode: {
        mode: COLOR_MODES.None,
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

        case actions.SHOW_NODES:
            const prev = state.hiddenNodeIds;
            return {
                ...state,
                hiddenNodeIds: prev
                    .filter(id => !action.payload.nodeIds.includes(parseInt(id)))
            };

        case actions.CLEAR_HIDDEN_NODES: {
            return {
                ...state,
                hiddenNodeIds: initialState().hiddenNodeIds
            };
        }

        case actions.SET_NODE_COLOR_MODE: {
            const newState = { ...state.nodeColors };
            const isGlobalAction = action.payload.elementIds.length === 0;
            const setId = isGlobalAction
                ? 0 // global set
                : state.nodeColors.setCount// new colorset;
            newState.setCount = isGlobalAction
                ? newState.setCount
                : newState.setCount + 1;
            const newSet = { mode: action.payload.mode, data: action.payload.data };

            action.payload.elementIds.forEach(id => newState.mapping[id] = setId);
            const presentSets = [...new Set([0, ...Object.values(newState.mapping)])];
            newState.colorSets = Object.fromEntries(presentSets.map(sid => [sid, newState.colorSets[sid]]));
            newState.colorSets[setId] = newSet;

            return {
                ...state,
                nodeColors: { ...newState },
            };
        }

        case actions.SET_EDGE_COLOR_MODE: {
            const newState = { ...state.edgeColors };
            const isGlobalAction = action.payload.elementIds.length === 0;
            const setId = isGlobalAction
                ? 0 // global set
                : state.edgeColors.setCount// new colorset;
            newState.setCount = isGlobalAction
                ? newState.setCount
                : newState.setCount + 1;
            const newSet = { mode: action.payload.mode, data: action.payload.data };

            action.payload.elementIds.forEach(id => newState.mapping[id] = setId);
            const presentSets = [...new Set([0, ...Object.values(newState.mapping)])];
            newState.colorSets = Object.fromEntries(presentSets.map(sid => [sid, newState.colorSets[sid]]));
            newState.colorSets[setId] = newSet;

            return {
                ...state,
                edgeColors: { ...newState },
            };
        }

        default:
            return state;
    }
}
