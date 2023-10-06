import React from 'react';
import * as utils from './utils';
import { renderToString } from 'react-dom/server';

import CytoscapeComponent from "react-cytoscapejs";
import cytoscape from 'cytoscape';
import cola from 'cytoscape-cola';
import fcose from 'cytoscape-fcose'
import euler from 'cytoscape-euler'
import dagre from 'cytoscape-dagre'
import nodeHtmlLabel from 'cytoscape-node-html-label';
import * as actions from '../App/actions';
import * as thunks from '../App/thunks';
import { useDispatch, useSelector } from 'react-redux';
import { Backdrop, CircularProgress } from '@mui/material';

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
    const [cy, setCy] = React.useState();
    const [, setAlertingNode] = React.useState(null);
    const [cyReady, setCyReady] = React.useState(false);

    const dbName = useSelector((state) => state.dbName);
    const cyLayout = useSelector((state) => state.cyLayout);
    const hiddenNeighbors = useSelector((state) => state.hiddenNeighbors);

    const previousElements = React.useRef([]);

    const cySetup = (cyState) => {
        props.cy.current = cyState;
        setCy(cyState)
    };

    const { data: cyStyle } = utils.useCyStyleQuery();
    const { data: rawElements, isFetching } = utils.useCytoscapeElements();

    const elements = rawElements || previousElements.current;
    previousElements.current = elements;
    props.currentElements.current = elements;

    const getCollapsed = React.useCallback(
        (id) => hiddenNeighbors.selectedNodeIds.includes(id)
        , [hiddenNeighbors]);

    React.useEffect(() => {
        if (!cy || cyReady) return;

        cy.nodeHtmlLabel([
            {
                query: 'node', // cytoscape query selector
                halign: 'center', // title vertical position. Can be 'left',''center, 'right'
                valign: 'center', // title vertical position. Can be 'top',''center, 'bottom'
                halignBox: 'center', // title vertical position. Can be 'left',''center, 'right'
                valignBox: 'center', // title relative box vertical position. Can be 'top',''center, 'bottom'
                cssClass: '', // any classes will be as attribute of <div> container for every title
                tpl(data) {
                    return renderToString(<>
                        {data.bloated ? <Dots nodeType={data.type} /> : <></>}
                    </>);
                }
            }
        ]);

        cy.on('onetap', event => {
            dispatch(thunks.inspectNode(dbName, null));

            const target = event.target;
            if (!target.group) {
                return;
            };
            if (target.group() !== "nodes") {
                return;
            };

            const rawNode = event.target.data();
            dispatch(thunks.inspectNode(dbName, rawNode.id));
        })

        cy.on('cxttap', event => {
            const target = event.target;
            const e = event.originalEvent;
            const newEvent = new e.constructor("contextmenu", e);

            if (!target.group) {
                props.setMenuData.current({group: "background", data: target.elements()});
            }

            else if (target.group() === "nodes") {
                props.setMenuData.current({group: "nodes", data: target.data()});
            }

            else {
                props.setMenuData.current({group: "edges", data: target.data()});
            }

            props.portalRef.current.dispatchEvent(newEvent);
        })

        cy.on('dbltap', event => {
            const target = event.target;
            if (!target.group) {
                cy.fit();
                return;
            }

            const node = target.data();
            if (target.group() !== 'nodes') {
                cy.fit();
                return;
            }

            dispatch(thunks.getNodes(dbName, [node.id], {
                yield_edges: true
            }))
                .then(res => {
                    setAlertingNode(res[node.id]);
                    dispatch(actions.selectNode(res[node.id]));
                });
        });

        cy.on('render', () => {
            if (!cy) return;

            const viewport = cy.getFitViewport();
            const zoom = viewport?.zoom;

            if (!zoom) return;

            cy.minZoom(zoom / 2);
            cy.maxZoom(zoom * 10);
        })

        setCyReady(true);
    }, [props, cy, cyReady, dbName, dispatch, getCollapsed]);

    React.useEffect(() => {
        if (!cy) return;

        if (props.layout.current) {
            props.layout.current.stop();
            props.layout.current.destroy();
        }

        cy.userZoomingEnabled(!cyLayout.fit);
        cy.userPanningEnabled(!cyLayout.fit);
        props.layout.current = cy.layout(cyLayout);
        props.layout.current.run();

    }, [cy, cyLayout, isFetching, props.layout, elements]);

    return <>
        {<Backdrop open={isFetching}><CircularProgress s={40} /></Backdrop>}
        <CytoscapeComponent
            elements={elements}
            cy={cySetup}
            stylesheet={cyStyle}
            style={{
                width: "100%",
                height: "100%"
            }}
        />
    </>;

}
export default CytoscapeCanvas;

