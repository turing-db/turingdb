import React from 'react';
import * as utils from '../utils';
import { renderToString } from 'react-dom/server';

import CytoscapeComponent from "react-cytoscapejs";
import cytoscape from 'cytoscape';
import cola from 'cytoscape-cola';
import fcose from 'cytoscape-fcose'
import euler from 'cytoscape-euler'
import dagre from 'cytoscape-dagre'
import nodeHtmlLabel from 'cytoscape-node-html-label';
import * as actions from '../../App/actions';
import * as thunks from '../../App/thunks';
import { useDispatch, useSelector } from 'react-redux';
import { Backdrop, CircularProgress } from '@mui/material';
import { VisualizerContext } from '../context';

import { Chart } from 'regraph';
import { useTheme } from '@emotion/react';

if (typeof cytoscape("core", "cola") === "undefined") {
    cytoscape.use(cola);
}
if (typeof cytoscape("core", "dagre") === "undefined") {
    cytoscape.use(dagre);
}
if (typeof cytoscape("core", "fcose") === "undefined") {
    cytoscape.use(fcose);
}
if (typeof cytoscape("core", "euler") === "undefined") {
    cytoscape.use(euler);
}
if (typeof cytoscape("core", "nodeHtmlLabel") === "undefined") {
    cytoscape.use(nodeHtmlLabel);
}

const Dots = (props) => <div style={{ display: "flex" }}>
    <box style={{
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        backgroundColor: "#e33",
        overflow: "hidden",
        position: "relative",
        borderRadius: "4px",
        opacity: props.nodeType === "selected" ? 0.95 : 0.5,
        bottom: "10px",
        width: "15px",
        height: "8px",
        fontSize: "6pt",
    }}>
        <span style={{ marginTop: "-4px" }}>...</span>
    </box>
</div>

const CytoscapeCanvas = (props) => {
    const dispatch = useDispatch();
    const theme = useTheme();
    const { setInteractionDisabled, fitRequired } = props;
    const [cyReady, setCyReady] = React.useState(false);
    const visualizer = React.useContext(VisualizerContext);

    const dbName = useSelector(state => state.dbName);
    const hiddenNeighbors = useSelector(state => state.hiddenNeighbors);
    const cyLayoutRef = React.useRef(visualizer.state.layouts.cyLayout);
    const layoutsRef = React.useRef(visualizer.state.layouts.layouts);
    const defaultLayoutRef = React.useRef();

    const previousElements = React.useRef([]);

    const { data: cyStyle } = utils.useCyStyleQuery();
    const { data: rawElements, isFetching } = utils.useCytoscapeElements();
    const elements = rawElements || previousElements.current;


    previousElements.current = elements;
    props.currentElements.current = elements;

    console.log("Rendering")
    const settings = {
        options: {
            navigation: false,
            overview: true,
            backgroundColor: theme.palette.background.default,
            fontSize: 60,
        },
        layout: {
            name: "organic",
        },
    };
    const items = React.useMemo(() => ({
        ...Object.fromEntries(elements
            .filter(e => e.group === "nodes")
            .map(n => ["node" + n.id, {
                data: {
                    ...n.data,
                    ...n.data.properties,
                },
                ...n.data,
                color: n.data.type === "selected" ? "rgb(204,204,204)" : "rgb(155,155,155)",
                size: 1,
                label: {
                    text: n.data.label,
                    backgroundColor: "rgba(0,0,0,0)",
                    color: n.data.type === "selected" ? "rgb(204,204,204)" : "rgb(155,155,155)",
                },
            }])),

        ...Object.fromEntries(elements
            .filter(e => e.group === "edges")
            .map(e => ["edge" + e.id, {
                data: e.data,
                ...e.data,
                id1: "node" + e.data.source,
                id2: "node" + e.data.target,
                color: e.data.type === "connecting" ? "rgb(0,230,0)" : "rgb(0,130,0)",
                end1: { arrow: true },
                width: 6,
                maxLength: 10,
                label: {
                    text: e.data.label,
                    backgroundColor: "rgba(0,0,0,0)",
                    color: e.data.type === "connecting" ? "rgb(0,230,0)" : "rgb(0,130,0)",
                },
            }]))
    }), [elements]);

    const getCollapsed = React.useCallback(
        (id) => hiddenNeighbors.selectedNodeIds.includes(id)
        , [hiddenNeighbors]);

    React.useEffect(() => {
        cyLayoutRef.current = visualizer.state.layouts.cyLayout;
        layoutsRef.current = visualizer.state.layouts.layouts;

        if (!visualizer.cy()) return;
        if (cyReady) return;

    }, [
        visualizer,
        props,
        cyReady,
        dbName,
        dispatch,
        getCollapsed,
    ]);

    React.useEffect(() => {
        if (!visualizer.cy()) return;

        const pan = { ...visualizer.cy().viewport().pan() };
        const zoom = visualizer.cy().viewport().zoom();
        //utils.applyLayouts(visualizer.cy(), visualizer.state.layouts.layouts, cyLayoutRef.current);
        visualizer.cy().pan(pan);
        visualizer.cy().zoom(zoom);

        setInteractionDisabled((() => {
            const nodes = visualizer.cy().nodes().filter(n => n.id);
            if (nodes.length === 0) return true;

            const uniqueTitleValues = nodes
                .map(n => n.data().properties.title)
                .filter((title, i, arr) => arr.indexOf(title) === i);

            if (uniqueTitleValues.length !== 2 && uniqueTitleValues.length !== 3) return true;

            return false;
        })());

        if (fitRequired.current) {
            visualizer.cy().fit();
            fitRequired.current = false;
        }
    }, [
        visualizer,
        setInteractionDisabled,
        elements,
        fitRequired
    ])

    return <>
        {<Backdrop open={isFetching}><CircularProgress s={40} /></Backdrop>}
        {<Chart
            items={items}
            {...settings}
            onDoubleClick={(e) => {
                dispatch(thunks.getNodes(dbName, [items[e.id].turing_id], { yield_edges: true }))
                    .then(res => dispatch(actions.selectNodes(Object.values(res))));
            }}
        />}
        {/*<div id="cy" style={{ width: "100%", height: "100%" }}>
        </div>*/}
    </>;

}
export default CytoscapeCanvas;

