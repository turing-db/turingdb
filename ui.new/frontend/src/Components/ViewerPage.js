import React from 'react';
import axios from 'axios';
import {
    Box,
    Autocomplete, TextField,
    Typography, Slider, Button, Modal, IconButton
} from '@mui/material';
import { useDispatch, useSelector } from 'react-redux';
import * as actions from '../App/actions';
import * as thunks from '../App/thunks';
import { useQuery } from '../App/queries';

import DeleteIcon from '@mui/icons-material/Delete'
import CytoscapeCanvas from '../Cytoscape/CytoscapeCanvas'
import CytoscapeContextMenu from '../Cytoscape/CytoscapeContextMenu'
import BorderedContainer, { BorderedContainerTitle } from './BorderedContainer';
import NodeStack from './NodeStack';
import NodeChip from './NodeChip';

const usePropertyTypes = (dbName) => {
    const dispatch = useDispatch();
    const displayedProperty = useSelector((state) => state.displayedProperty);

    const { data: rawPropertyTypes } = useQuery(
        ["list_property_types_viewer", dbName],
        React.useCallback(() => axios
            .post("/api/list_property_types", { db_name: dbName })
            .then(res => res.data)
            .catch(err => { console.log(err); return []; })
            , [dbName]),
    );
    const propertyTypes = React.useMemo(() => rawPropertyTypes || [], [rawPropertyTypes]);
    const namingProps = ["displayName", "name", "Name", "NAME"]
        .filter(p => propertyTypes.includes(p));

    const defaultProperty = React.useMemo(() => {
        if (namingProps[0]) return namingProps[0];

        const regexProps = propertyTypes.map(p => p.match("/name|Name|NAME/"));
        return regexProps.length !== 0
            ? regexProps[0]
            : propertyTypes[0];
    }, [namingProps, propertyTypes])

    React.useEffect(() => {
        if (displayedProperty === null && defaultProperty !== undefined) {
            dispatch(actions.selectDisplayedProperty(defaultProperty))
        }
    }, [defaultProperty, dispatch, displayedProperty]);


    return { propertyTypes, displayedProperty };
}
const HiddenNodesContainer = () => {
    const hiddenNodeIds = useSelector(state => state.hiddenNodeIds);
    const dispatch = useDispatch();

    return <BorderedContainer title={
        <BorderedContainerTitle title="Hidden nodes">
            {hiddenNodeIds.length !== 0 &&
                <IconButton onClick={() => { dispatch(actions.clearHiddenNodes()) }}>
                    <DeleteIcon />
                </IconButton>}
        </BorderedContainerTitle>
    }>
        {hiddenNodeIds.length !== 0
            ? <NodeStack>
                {hiddenNodeIds.map((id, i) => <NodeChip key={i} nodeId={id} />)}
            </NodeStack>
            : "None"}
    </BorderedContainer>;
};

const HiddenNodesModal = (props) => {
    const onClose = () => props.setOpen(false);
    const style = {
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        width: 400,
        bgcolor: 'background.paper',
        border: '2px solid #000',
        boxShadow: 24,
        p: 4,
    };

    return (
        <Modal open={props.open} onClose={onClose}>
            <Box sx={style}>
                <HiddenNodesContainer />
            </Box>
        </Modal>
    );
}

export default function ViewerPage() {
    const dispatch = useDispatch();

    const dbName = useSelector((state) => state.dbName);
    const cyLayout = useSelector((state) => state.cyLayout);

    const [showHiddenNodes, setShowHiddenNodes] = React.useState(false);
    const portalRef = React.useRef();
    const layout = React.useRef();
    const cy = React.useRef();
    const currentElements = React.useRef({});
    const portalData = React.useRef({});
    const setMenuData = React.useRef(() => console.log("NOT SET"));

    const { propertyTypes, displayedProperty } = usePropertyTypes(dbName);

    return <Box p={0} m={0} display="flex" flexDirection="column" flex={1}>
        <Box p={1} display="flex" flexDirection="row" alignItems="center">
            <Button
                variant="contained"
                onClick={() => {
                    const nodeIds = Object
                        .values(currentElements.current)
                        .filter(el => el.group === "nodes")
                        .map(el => el.data.id);

                    dispatch(thunks.getNodes(dbName, nodeIds, { yield_edges: true }))
                        .then(res => dispatch(actions.selectNodes(Object.values(res))));
                }}
            >
                Select all visible nodes
            </Button>

            <Box pl={2} pr={2}>
                <Typography gutterBottom>Edge length</Typography>
                <Slider valueLabelDisplay="auto" aria-label="Edge length"
                    value={cyLayout.edgeLengthVal} min={5} max={800}
                    onChange={(_e, v) => dispatch(actions.setCyLayout({
                        ...cyLayout,
                        edgeLengthVal: v
                    }))}
                />
            </Box>

            <Autocomplete
                disablePortal blurOnSelect id="property-selector"
                value={displayedProperty}
                autoSelect autoHighlight options={propertyTypes}
                sx={{ width: 200 }} size="small"
                onChange={(_e, v) => v &&
                    dispatch(actions.selectDisplayedProperty(v))}
                renderInput={(params) => (
                    <TextField {...params} label="Property" />)}
            />

            <Box pl={2}>
                <Button size="small" onClick={() => cy.current.fit()} variant="contained">
                    Fit
                </Button>
            </Box>

            <Box pl={2}>
                <Button
                    variant="contained"
                    size="small"
                    onClick={() => {
                        layout.current = cy.current.layout(cyLayout);
                        layout.current.run();
                    }}
                >
                    Clean
                </Button>
            </Box>
            <Button onClick={() => setShowHiddenNodes(true)}>Hidden nodes</Button>
        </Box>
        <HiddenNodesModal open={showHiddenNodes} setOpen={setShowHiddenNodes} />
        <CytoscapeCanvas {...{ cy, layout, portalData, portalRef, currentElements }} setMenuData={setMenuData} />
        <CytoscapeContextMenu
            ref={portalRef}
            data={portalData}
            setMenuData={setMenuData} />
    </Box>;
}

