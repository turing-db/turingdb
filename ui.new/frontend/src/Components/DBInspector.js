import React from 'react';

import {
    Box
} from '@mui/material'

import {
    NodeTypeFilterContainer,
    NodeFilterContainer,
    SelectedNodesContainer,
} from './'

export default function DBInspector() {
    const [selectedNodeType, setSelectedNodeType] = React.useState(null);

    return <Box>
        <NodeTypeFilterContainer
            selected={selectedNodeType}
            setSelected={setSelectedNodeType}
        />
        <NodeFilterContainer
            selectedNodeType={selectedNodeType}
        />
        <SelectedNodesContainer
        />
    </Box>;
}
