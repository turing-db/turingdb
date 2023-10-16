import React from 'react';

import {
    Box
} from '@mui/material'

import NodeTypeFilterContainer from './NodeTypeFilterContainer';
import PropertyFilterContainer from './PropertyFilterContainer';
import NodeFilterContainer from './NodeFilterContainer';
import SelectedNodesContainer from './SelectedNodesContainer';

export default function DBInspector() {
    const [selectedNodeType, setSelectedNodeType] = React.useState(null);
    const [propertyValue, setPropertyValue] = React.useState("");

    return <Box>
        <NodeTypeFilterContainer
            selected={selectedNodeType}
            setSelected={setSelectedNodeType}
        />

        <PropertyFilterContainer
            propertyValue={propertyValue}
            setPropertyValue={setPropertyValue}
        />

        <NodeFilterContainer
            selectedNodeType={selectedNodeType}
            propertyValue={propertyValue}
        />

        <SelectedNodesContainer />
    </Box>;
}

