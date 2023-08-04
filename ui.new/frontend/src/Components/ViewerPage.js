import { AppContext } from './App'
import React from 'react'
import { Box, Autocomplete, CircularProgress, TextField } from '@mui/material';
import axios from 'axios'
import CytoscapeComponent from 'react-cytoscapejs';
import { useTheme } from '@emotion/react';
import cytoscape from 'cytoscape';
import cola from 'cytoscape-cola'
import dagre from 'cytoscape-dagre'
import elk from 'cytoscape-elk'
import klay from 'cytoscape-klay'
import cise from 'cytoscape-cise'
import { SelectedNodesContainer } from './'

async function getCyStyle() {
    return await fetch('/cy-style.json')
        .then(function(res) {
            return res.json();
        });
}

export default function ViewerPage() {
    const context = React.useContext(AppContext);
    const theme = useTheme();
    const [cyStyle, setCyStyle] = React.useState(null);
    const [edges, setEdges] = React.useState([]);
    const [loading, setLoading] = React.useState(true);

    cytoscape.use(cola);
    cytoscape.use(dagre);
    cytoscape.use(elk);
    cytoscape.use(klay);
    cytoscape.use(cise);

    if (!context.currentDb) {
        return <Box>Select a database to start</Box>;
    }
    console.log(context.selectedNodes.nodes);

    if (loading) {
        const ids = [
            ...context.selectedNodes.nodes.map(
                n => n.ins
            ),
            ...context.selectedNodes.nodes.map(
                n => n.outs
            )
        ].flat();

        if (ids.length !== 0) {
            axios
                .get("/api/list_edges", {
                    params: {
                        db_name: context.currentDb,
                        ids: ids.reduce((f, s) => `${f},${s}`)
                    }
                })
                .then(res => {
                    setLoading(false);
                    if (res.data.error) {
                        console.log(res.data.error);
                        setEdges([])
                        return;
                    }
                    setEdges(res.data.filter(e =>
                        context.selectedNodes.hasId(e.source_id)
                        && context.selectedNodes.hasId(e.target_id)
                    ));
                })
                .catch(err => {
                    setLoading(false);
                    setEdges([]);
                    console.log("Error: ", err);
                });

            return <Box>Loading edges <CircularProgress s={20} /></Box>
        }
    }

    if (!cyStyle) {
        getCyStyle().then(res => setCyStyle(res));
        return <Box>Loading cytoscape <CircularProgress s={20} /></Box>
    }

    const formatedNodes = context.selectedNodes.nodes.map(n => {
        return {
            data: { id: n.id, name: n.id },
            group: "nodes"
        };
    });
    const formatedEdges = edges.map(e => {
        return {
            data: { id: e.id, source: e.source_id, target: e.target_id },
            group: "edges"
        }
    });

    const elements = [
        ...formatedNodes,
        ...formatedEdges
    ];

    const layouts = [
        "cola",
        "dagre",
        "concentric",
        "elk",
        "klay",
        "cise",
    ];

    return <Box>
        <SelectedNodesContainer />
        <Box
            p={1}
            m={1}
            border={1}
            borderRadius={4}
            overflow="hidden"
            borderColor={theme.palette.background.paper}
            bgcolor={theme.palette.background.paper}
            flex={1}
            height="84vh"
        >
            <Autocomplete
                value={context.selectedCyLayout}
                onChange={(_e, newLayout) => context.setSelectedCyLayout(newLayout)}
                disablePortal
                id="select-layout"
                options={layouts}
                sx={{ width: 300 }}
                renderInput={(params) => <TextField {...params} label="Graph layout" />}
            />
            <CytoscapeComponent
                elements={elements}
                layout={{
                    name: context.selectedCyLayout
                }}
                stylesheet={cyStyle}
                style={{
                    width: "100%",
                    height: "100%",
                }}
            />
        </Box>
    </Box >
}
