import React from 'react';
import axios from 'axios';
import {
    Box,
    Modal, Alert, Autocomplete, CircularProgress, TextField,
    Typography, Checkbox, FormControlLabel, Slider, Button, Backdrop
} from '@mui/material';
import CytoscapeComponent from 'react-cytoscapejs';
import cytoscape from 'cytoscape';
import cola from 'cytoscape-cola';
import nodeHtmlLabel from 'cytoscape-node-html-label';
import { useDispatch, useSelector } from 'react-redux';
import * as actions from '../App/actions';
import * as thunks from '../App/thunks';
import { useQuery } from '../App/queries';
import { renderToString } from 'react-dom/server';

const edgeCountLim = 40;

if (typeof cytoscape("core", "cola") === "undefined") {
    cytoscape.use(cola);
}
if (typeof cytoscape("core", "nodeHtmlLabel") === "undefined") {
    cytoscape.use(nodeHtmlLabel);
}

const getCyStyle = async () => {
    return await fetch('/cy-style.json')
        .then(res => res.json())
        .catch(err => console.log(err));
}

const defaultData = () => ({ selectedNodes: {}, neighbors: {}, edges: {} });

export default function ViewerPage() {
    const selectedNodes = useSelector((state) => state.selectedNodes);
    const dbName = useSelector((state) => state.dbName);
    const cyLayout = useSelector((state) => state.cyLayout);
    const layout = React.useRef(null);
    const [alertingNode, setAlertingNode] = React.useState(null);
    const [property, setProperty] = React.useState(null);
    const [cy, setCy] = React.useState(null);
    const [cyReady, setCyReady] = React.useState(false);
    const previousData = React.useRef(defaultData());
    const dispatch = useDispatch();

    const { data: cyStyle, isFetching: isFetchingStyle } = useQuery(
        ["get_cystyle"],
        getCyStyle,
    )

    const cySetup = React.useCallback(cyState => {
        setCy(cyState);
    }, []);

    const { data: rawProperties } = useQuery(
        ["list_string_property_types_viewer", dbName],
        React.useCallback(() => axios
            .post("/api/list_string_property_types", { db_name: dbName })
            .then(res => res.data)
            .catch(err => { console.log(err); return []; })
            , [dbName]),
    );
    const properties = React.useMemo(() => rawProperties || [], [rawProperties]);

    React.useEffect(() => {
        if (properties.length === 0) return;

        if (properties.includes("displayName")) setProperty("displayName");
        else if (properties.includes("name")) setProperty("name");
        else {
            const matches = properties.map(p => p.match("/name|Name|NAME/"));
            if (matches.length !== 0) setProperty(matches[0])
            else setProperty(properties[0])
        }
    }, [properties])

    React.useEffect(() => {
        if (!cy || cyReady) return;

        cy.nodeHtmlLabel([
            {
                query: 'node[type="selected"]', // cytoscape query selector
                halign: 'center', // title vertical position. Can be 'left',''center, 'right'
                valign: 'center', // title vertical position. Can be 'top',''center, 'bottom'
                halignBox: 'center', // title vertical position. Can be 'left',''center, 'right'
                valignBox: 'center', // title relative box vertical position. Can be 'top',''center, 'bottom'
                cssClass: '', // any classes will be as attribute of <div> container for every title
                tpl(data) {
                    const edgeCount = parseInt(data.inCount) + parseInt(data.outCount);
                    return edgeCount > edgeCountLim
                        ? renderToString(
                            <div style={{
                                display: "flex",
                            }}>
                                <box style={{
                                    display: "flex",
                                    justifyContent: "center",
                                    alignItems: "center",
                                    backgroundColor: "#e33",
                                    overflow: "hidden",
                                    position: "relative",
                                    borderRadius: "4px",
                                    opacity: 0.95,
                                    left: "10px",
                                    bottom: "10px",
                                    width: "15px",
                                    height: "8px",
                                    fontSize: "6pt",
                                }}>
                                    <span style={{ marginTop: "-4px" }}>...</span>
                                </box>

                            </div>
                        )
                        : <></>;
                }
            }
        ]);

        cy.on('onetap', event => {
            dispatch(actions.inspectNode(null));

            const target = event.target;
            if (!target.group) {
                return;
            };
            if (target.group() !== "nodes") {
                return;
            };

            const node = event.target.data();
            dispatch(actions.inspectNode(node.turingData));
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

            setAlertingNode(node);
            dispatch(actions.selectNode(node.turingData));
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
    }, [cy, cyLayout, cyReady, dispatch, selectedNodes])

    const edgeIds = React.useMemo(() => Object
        .values(selectedNodes)
        .map(n => {
            const ids = [...n.ins, ...n.outs];
            return ids.length > edgeCountLim ? ids.slice(0, edgeCountLim) : ids;
        })
        .flat()
        .filter((id, i, arr) => arr.indexOf(id) === i),
        [selectedNodes]);

    const getData = React.useCallback(async () => {
        const edges = await dispatch(thunks
            .getEdges(dbName, edgeIds))
            .then(res => res);

        const nodeIds = Object
            .values(edges)
            .map(e => [e.source_id, e.target_id])
            .flat()
            .filter(id => !(selectedNodes.hasOwnProperty(id)));

        const ids = [...Object.keys(selectedNodes), ...nodeIds];

        const nodeProperties = await axios
            .post("/api/list_nodes_properties", {
                db_name: dbName,
                ids: ids,
            })
            .then(res => Object.fromEntries(Object
                .entries(res.data)
                .map(([id, props]) => [id, props[property] || ""])))
            .catch(err => {
                console.log(err);
                return {};
            })

        const neighbors = await dispatch(thunks
            .getNodes(dbName, nodeIds))
            .then(nodes => Object.fromEntries(Object
                .values(nodes)
                .map(n => [n.id, {
                    id: n.id,
                    label: (() => {
                        const res = nodeProperties[n.id];
                        if (!res) return "";
                        if (res.length > 18) return res.slice(0, 15) + "...";
                        return res;
                    })(),
                    turingData: n,
                    inCount: n.ins.length,
                    outCount: n.outs.length,
                    type: "neighbor"
                }])
            ));

        const selNodes = Object.fromEntries(Object
            .values(selectedNodes)
            .map(n => [n.id, {
                id: n.id,
                label: (() => {
                    const res = nodeProperties[n.id];
                    if (!res) return "";
                    if (res.length > 10) return res.slice(0, 15) + "...";
                    return res;
                })(),
                turingData: n,
                inCount: n.ins.length,
                outCount: n.outs.length,
                type: "selected"
            }])
        )

        return { selectedNodes: selNodes, neighbors, edges };
    }, [dispatch, dbName, selectedNodes, edgeIds, property]);

    const { data, isFetching } = useQuery(
        ["get_data", dbName, edgeIds, property],
        getData,
        { refetchOnMount: true, staleTime: 0 }
    );

    const rawData = data || previousData.current;
    previousData.current = { ...rawData };

    const elements = React.useMemo(() =>
        [
            ...Object.values({
                ...Object.fromEntries(
                    Object
                        .values(rawData.neighbors)
                        .map(n => [n.id, { data: n, group: "nodes" }])),
                ...Object.fromEntries(
                    Object
                        .values(rawData.selectedNodes)
                        .map(n => [n.id, { data: n, group: "nodes" }])),
            }),
            ...Object
                .values(rawData.edges)
                .map(e => ({
                    data: { turingId: e.id, source: e.source_id, target: e.target_id },
                    group: "edges"
                }))
        ], [rawData]);

    React.useEffect(() => {
        if (!cy) return;

        if (layout.current) {
            layout.current.stop();
            layout.current.destroy();
        }

        cy.userZoomingEnabled(!cyLayout.fit);
        cy.userPanningEnabled(!cyLayout.fit);
        layout.current = cy.layout({
            ...cyLayout,
            edgeLength: (_e) => cyLayout.edgeLengthVal
        });
        layout.current.run();

    }, [cy, cyLayout, elements]);

    if (!dbName) {
        return <Box>Select a database to start</Box>;
    }


    return <>
        <Backdrop open={isFetching}><CircularProgress s={40} /></Backdrop>
        <Modal
            open={(() => alertingNode !== null && alertingNode.inCount + alertingNode.outCount > edgeCountLim)()}
            onClose={() => setAlertingNode(null)}
            aria-labelledby="alertingEdgeCount-modal-title"
            aria-describedby="alertingEdgeCount-modal-description"
        >
            <Box sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                width: 400,
                bgcolor: 'background.paper',
                border: '2px solid #000',
                boxShadow: 24,
                borderRadius: "2px",
                p: 4,
            }}>
                <Alert severity="warning">
                    The node has too many connections ({
                        alertingNode?.inCount + alertingNode?.outCount
                    }).
                    Showing all the edges would hurt performances
                </Alert>
            </Box>
        </Modal>

        {isFetchingStyle
            ? <Box>
                <Typography variant="h4">
                    Loading cytoscape... <CircularProgress s={20} />
                </Typography>
            </Box>
            : <Box
                p={0}
                m={0}
                display="flex"
                flexDirection="column"
                flex={1}
            >
                <Box
                    p={1}
                    display="flex"
                    flexDirection="row"
                    alignItems="center"
                >
                    <FormControlLabel
                        label="Lock viewport"
                        labelPlacement="start"
                        control={
                            <Checkbox
                                checked={cyLayout.fit}
                                onChange={() => {
                                    dispatch(actions.setCyLayout({
                                        ...cyLayout,
                                        fit: !cyLayout.fit,
                                    }));
                                }}
                            />}
                    />

                    <Box pl={2} pr={2}>
                        <Typography gutterBottom>Edge length</Typography>
                        <Slider
                            valueLabelDisplay="auto"
                            aria-label="Edge length"
                            value={cyLayout.edgeLengthVal}
                            onChange={(_e, v) => dispatch(actions.setCyLayout({
                                ...cyLayout,
                                edgeLengthVal: v
                            }))}
                            min={5}
                            max={400}
                        />
                    </Box>

                    <Autocomplete
                        disablePortal
                        blurOnSelect
                        id="property-selector"
                        value={property}
                        onChange={(_e, v) => v && setProperty(v)}
                        autoSelect
                        autoHighlight
                        options={properties}
                        sx={{ width: 300 }}
                        size="small"
                        renderInput={(params) => (
                            <TextField
                                {...params}
                                label="Property"
                            />
                        )}
                    />

                    <Box pl={2}>
                        <Button
                            onClick={() => cy.fit()}
                            variant="contained"
                        >
                            Fit
                        </Button>
                    </Box>

                    <Box pl={2}>
                        <Button
                            onClick={() => layout.current.run()}
                            variant="contained"
                        >
                            Clean
                        </Button>
                    </Box>
                </Box>
                <CytoscapeComponent
                    elements={elements}
                    cy={cySetup}
                    stylesheet={cyStyle}
                    style={{ width: "100%", height: "100%" }}
                />
            </Box>
        }
    </>
}
