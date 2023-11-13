import React from 'react';

export const SET_CY_LAYOUT = "SET_CY_LAYOUT";
export const ADD_LAYOUT = "ADD_LAYOUT";
export const SET_DEFAULT_LAYOUT = "SET_DEFAULT_LAYOUT";

export const layoutsInitialState = () => ({
    //cyLayout: { name: "dagre" },
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
        refresh: 2,
        edgeLength: (_e) => 150,
        lineCount: 0,
    },
    layouts: {
        definitions: {}, // Maps the layout definition to a unique id
        mapping: {}, // Maps the node ids to one of the layout ids
        layoutCount: 0,
    },
})

const useLayoutsReducer = (state, action) => {
    switch (action.type) {
        case "CLEAR":
            return layoutsInitialState();

        case SET_CY_LAYOUT: {
            return {
                ...state,
                cyLayout: {
                    ...action.payload.cyLayout,
                    edgeLength: (_e) => action.payload.cyLayout.edgeLengthVal,
                }
            };
        }

        case ADD_LAYOUT:
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
                },
            };

        case SET_DEFAULT_LAYOUT: {
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

        default:
            return state;
    }
}

const useLayouts = () => {
    const [state, dispatch] = React.useReducer(useLayoutsReducer, layoutsInitialState());
    const setCyLayout = React.useCallback(cyLayout => dispatch({
        type: SET_CY_LAYOUT,
        payload: { cyLayout }
    }), []);

    const addLayout = React.useCallback((definition, nodeIds) => dispatch({
        type: ADD_LAYOUT,
        payload: { definition, nodeIds },
    }), []);

    const setDefaultLayout = React.useCallback(nodeIds => dispatch({
        type: SET_DEFAULT_LAYOUT,
        payload: { nodeIds },
    }), []);


    return {
        cyLayout: state.cyLayout,
        layouts: state.layouts,
        setCyLayout,
        setDefaultLayout,
        addLayout,
    };
}

export default useLayouts;
