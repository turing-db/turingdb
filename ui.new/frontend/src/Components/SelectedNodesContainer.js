import { IconButton } from '@mui/material'
import DeleteIcon from '@mui/icons-material/Delete'
import { BorderedContainer, NodeChip, NodeStack } from './'
import React from 'react'
import { BorderedContainerTitle } from './BorderedContainer';
import { useSelector, useDispatch } from 'react-redux'
import { clearSelectedNodes } from '../App/actions'

export default function SelectedNodesContainer() {
    const selectedNodes = useSelector((state) => state.selectedNodes);
    const dispatch = useDispatch();

    return <BorderedContainer title={
        <BorderedContainerTitle title="Selected nodes">
            {selectedNodes.length !== 0 &&
                <IconButton onClick={() => {
                    dispatch(clearSelectedNodes())
                }}>
                    <DeleteIcon />
                </IconButton>}
        </BorderedContainerTitle>
    }>
        <NodeStack>
            {Object.values(selectedNodes).map(
                (node, i) =>
                    <NodeChip
                        node={node}
                        key={i}
                        nodeId={node.id}
                    />
            )}
        </NodeStack>
    </BorderedContainer >;
}
