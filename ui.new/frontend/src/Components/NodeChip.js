import React from 'react'

import { Grid, Chip } from '@mui/material'
import AddCircleSharpIcon from '@mui/icons-material/AddCircleSharp'
import CircleSharpIcon from '@mui/icons-material/CircleSharp';

import { useDispatch, useSelector } from 'react-redux';
import * as actions from '../App/actions'

const NodeChip = (props) => {
    const node = props.node;
    const selectedNodes = useSelector((state) => state.selectedNodes);
    const dispatch = useDispatch();

    //React.useEffect(() => {
    //    setNode(null);
    //}, [propNodeId])

    //React.useEffect(() => {
    //    if (!node) {
    //        dispatch(thunks.getNodes(dbName, [propNodeId]))
    //            .then(nodes => setNode(Object.values(nodes)[0]));
    //    }
    //}, [node, dbName, propNodeId, dispatch]);

    return <Grid item p={0.5}><Chip {...{
        label: props.nodeId,
        variant: "outlined",
        ...node
            ? {
                onClick: () => dispatch(actions.inspectNode(node)),
                ...(selectedNodes[node.id]
                    ? {
                        onDelete: () => {
                            dispatch(actions.unselectNode(node));
                        }
                    }
                    : {
                        onDelete: () => {
                            dispatch(actions.selectNode(node));
                        },
                        deleteIcon: <AddCircleSharpIcon />
                    }
                )
            }
            : {
                onClick: () => { },
                onDelete: () => { },
                deleteIcon: <CircleSharpIcon />
            }
    }} /></Grid>;
}

export default NodeChip
