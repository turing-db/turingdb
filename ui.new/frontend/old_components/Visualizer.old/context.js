import React from 'react'
import * as utils from './utils';
import useLayouts from './reducers/layouts';
import useEdgeLabel from './reducers/edgeLabel';
import useNodeColors from './reducers/nodeColors';
import useEdgeColors from './reducers/edgeColors';
import useHiddenNodeIds from './reducers/hiddenNodeIds';
import useFilters from './reducers/filters';


export const VisualizerContext = React.createContext();

export const VisualizerContextProvider = (props) => {
    const cyRef = React.useRef();
    const fitRequired = React.useRef();
    const portalRef = React.useRef();
    const setMenuData = React.useRef();
    const layouts = useLayouts();
    const edgeLabel = useEdgeLabel();
    const { setEdgeLabel, edgeLabel: edgeLabelState } = edgeLabel;
    const nodeColors = useNodeColors();
    const edgeColors = useEdgeColors();
    const hiddenNodeIds = useHiddenNodeIds();
    const filters = useFilters();
    const edgeLabelRef = React.useRef(edgeLabelState);

    React.useEffect(() => {
        edgeLabelRef.current = edgeLabelState;
    }, [edgeLabelState]);

    const visualizer = React.useMemo(() => ({
        callbacks: {
            showCellCellInteraction: () => utils.showCellCellInteraction(layouts.addLayout, cyRef.current, fitRequired),
            applyLayouts: () => utils.applyLayouts(cyRef.current, layouts.layouts, layouts.cyLayout),
            fit: () => cyRef.current.fit(),
            clearHiddenNodes: () => hiddenNodeIds.clearHiddenNodes(),
            setEdgeLabel: (v) => {
                v && setEdgeLabel(v);
            },
        },
        state: {
            layouts: layouts,
            hiddenNodeIds: hiddenNodeIds,
            edgeLabel: edgeLabel,
            nodeColors: nodeColors,
            edgeColors: edgeColors,
            filters,
            fitRequired,
            cyRef,
        },
        cy: () => cyRef.current,
        portalRef,
        setMenuData
    }), [
        fitRequired,
        setEdgeLabel,
        edgeLabel,
        hiddenNodeIds,
        nodeColors,
        edgeColors,
        filters,
        layouts,
    ]);

    return <VisualizerContext.Provider value={visualizer}>
        {props.children}
    </VisualizerContext.Provider>;
}
