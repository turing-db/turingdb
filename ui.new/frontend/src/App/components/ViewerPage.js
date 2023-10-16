import React from 'react';
import { useTheme } from '@emotion/react';
import {
    Box,
    Autocomplete, TextField,
    Button, Modal, IconButton, Tooltip
} from '@mui/material';
import { useDispatch, useSelector } from 'react-redux';
import * as actions from '../actions';
import * as thunks from '../thunks';
import { useNodePropertyTypesQuery, useEdgePropertyTypesQuery } from '../queries';
import { Visualizer } from '../../Visualizer';

import DeleteIcon from '@mui/icons-material/Delete'
import BorderedContainer, { BorderedContainerTitle } from './BorderedContainer';
import NodeStack from './NodeStack';
import NodeChip from './NodeChip';
import SettingsModal from '../../Visualizer/components/SettingsModal';
import VisibilityIcon from '@mui/icons-material/Visibility';
import CleaningServicesIcon from '@mui/icons-material/CleaningServices';
import FitScreenIcon from '@mui/icons-material/FitScreen';
import SettingsIcon from '@mui/icons-material/Settings';
import { Icon } from "@blueprintjs/core";

const usePropertyTypes = () => {
    const displayedNodeProperty = useSelector(state => state.displayedNodeProperty);
    const edgeLabel = useSelector(state => state.visualizer.edgeLabel);

    const { data: rawNodePropertyTypes } = useNodePropertyTypesQuery();
    const { data: rawEdgePropertyTypes } = useEdgePropertyTypesQuery();
    const nodePropertyTypes = React.useMemo(() => rawNodePropertyTypes || [], [rawNodePropertyTypes]);
    const edgePropertyTypes = React.useMemo(() => rawEdgePropertyTypes || [], [rawEdgePropertyTypes]);

    return { nodePropertyTypes, edgePropertyTypes, displayedNodeProperty, edgeLabel };
}

const HiddenNodesContainer = (props) => {
    const hiddenNodeIds = useSelector(state => state.visualizer.hiddenNodeIds);
    const { visualizer } = props;

    return <BorderedContainer title={
        <BorderedContainerTitle title="Hidden nodes">
            {hiddenNodeIds.length !== 0 &&
                <IconButton onClick={() => visualizer.current.callbacks.clearHiddenNodes()}>
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
                <HiddenNodesContainer visualizer={props.visualizer} />
            </Box>
        </Modal>
    );
}

export default function ViewerPage() {
    const dispatch = useDispatch();
    const theme = useTheme();

    const dbName = useSelector(state => state.dbName);
    const btnStyle = {
        size: "large",
        color: "primary",
    };

    const [showHiddenNodes, setShowHiddenNodes] = React.useState(false);
    const [showSettings, setShowSettings] = React.useState(false);
    const [interactionDisabled, setInteractionDisabled] = React.useState(true);
    const fitRequired = React.useRef(false);
    const currentElements = React.useRef({});
    const visualizer = React.useRef({});

    const {
        nodePropertyTypes,
        edgePropertyTypes,
        displayedNodeProperty,
        edgeLabel
    } = usePropertyTypes();

    const CellCellInteractionIcon = (props) => <Icon
        icon="intersection"
        color={props.disabled ? theme.palette.background.disabled : theme.palette.primary.main}
        size={25}
    />;


    return <Box p={0} m={0} display="flex" flexDirection="column" flex={1}>
        <Box p={1} display="flex" flexDirection="row" alignItems="center">

            <Box>
                <Tooltip title="Settings" arrow>
                    <IconButton {...btnStyle} onClick={() => setShowSettings(true)}>
                        <SettingsIcon />
                    </IconButton>
                </Tooltip>

                <Tooltip title="Fit canvas" arrow>
                    <IconButton {...btnStyle} onClick={() => visualizer.current.callbacks.fit()}>
                        <FitScreenIcon />
                    </IconButton>
                </Tooltip>

                <Tooltip title="Clean up canvas" arrow>
                    <IconButton {...btnStyle}
                        onClick={() => visualizer.current.callbacks.applyLayouts()}>
                        <CleaningServicesIcon />
                    </IconButton>
                </Tooltip>

                <Tooltip title="Hidden nodes" arrow>
                    <IconButton {...btnStyle} onClick={() => setShowHiddenNodes(true)}>
                        <VisibilityIcon />
                    </IconButton>
                </Tooltip>

                <Tooltip title="Show cell-cell interaction" arrow>
                    <span> {/*Span is necessary for material-ui <Tooltip>*/}
                        <IconButton
                            {...btnStyle}
                            disabled={interactionDisabled}
                            onClick={() => visualizer.current.callbacks.showCellCellInteraction()}
                        >
                            <CellCellInteractionIcon disabled={interactionDisabled} />
                        </IconButton>
                    </span>
                </Tooltip>
            </Box>

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

            <Autocomplete
                disablePortal
                blurOnSelect
                id="node-property-selector"
                value={displayedNodeProperty}
                autoSelect
                autoHighlight
                options={[...nodePropertyTypes, "NodeType", "None"]}
                sx={{ width: 200 }}
                size="small"
                onChange={(_e, v) => v && dispatch(actions.selectDisplayedProperty(v))}
                renderInput={(params) => (<TextField {...params} label="Displayed node property" />)}
            />

            <Autocomplete
                disablePortal
                blurOnSelect
                id="edge-property-selector"
                value={edgeLabel}
                autoSelect
                autoHighlight
                options={[...edgePropertyTypes, "EdgeType", "None"]}
                sx={{ width: 200 }}
                size="small"
                onChange={(_e, v) => visualizer.current.callbacks.setEdgeLabel(v)}
                renderInput={(params) => (<TextField {...params} label="Edge label" />)}
            />
        </Box>
        <HiddenNodesModal
            open={showHiddenNodes}
            setOpen={setShowHiddenNodes}
            visualizer={visualizer}
        />
        <SettingsModal open={showSettings} setOpen={setShowSettings} />
        <Visualizer
            {...{
                visualizer,
                currentElements,
                interactionDisabled,
                setInteractionDisabled,
                fitRequired,
            }}
        />
    </Box >;
}

