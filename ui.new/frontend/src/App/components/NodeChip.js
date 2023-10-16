import React from 'react'

import { Grid, Chip } from '@mui/material'
import AddCircleSharpIcon from '@mui/icons-material/AddCircleSharp'
import CircleSharpIcon from '@mui/icons-material/CircleSharp';
import DeleteSharpIcon from '@mui/icons-material/DeleteSharp';

import { useDispatch, useSelector } from 'react-redux';
import * as actions from '../actions'
import * as thunks from '../thunks'

const NodeChip = (props) => {
    const node = props.node;
    const selectedNodes = useSelector(state => state.selectedNodes);
    const displayedNodeProperty = useSelector(state => state.displayedNodeProperty);
    const dbName = useSelector(state => state.dbName);
    const dispatch = useDispatch();

    const onClick = () => dispatch(thunks.inspectNode(dbName, props.nodeId));

    const onDelete = node
        ? selectedNodes[node.id]
            ? () => dispatch(actions.unselectNode(node))
            : () => dispatch(actions.selectNode(node))
        : () => { };

    const deleteIcon = node
        ? selectedNodes[node.id]
            ? <DeleteSharpIcon />
            : <AddCircleSharpIcon />
        : <CircleSharpIcon />;

    const label = node?.properties[displayedNodeProperty] || "Node " + props.nodeId;

    return <Grid item p={0.5}>
        <Chip
            onClick={onClick}
            onDelete={onDelete}
            deleteIcon={deleteIcon}
            variant="outlined"
            label={label}
        />
    </Grid>;
}

export default NodeChip

