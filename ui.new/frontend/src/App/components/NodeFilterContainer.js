import {
    Box,
    Button,
    CircularProgress,
    TextField,
    Alert,
} from '@mui/material'
import React from 'react'
import NodeStack from './NodeStack'; 
import BorderedContainer from './BorderedContainer';
import NodeInspector from './NodeInspector';
import NodeChip from './NodeChip';
import { BorderedContainerTitle } from './BorderedContainer'
import { useDispatch, useSelector } from 'react-redux'
import * as actions from '../actions'
import * as thunks from '../thunks'
import { useQuery } from '../queries'


const Title = (props) => {
    const { setError } = props;
    const [addNodeId, setAddNodeId] = React.useState("")
    const selectedNodes = useSelector((state) => state.selectedNodes);
    const dbName = useSelector((state) => state.dbName);
    const dispatch = useDispatch();


    return <BorderedContainerTitle title="Nodes">
        <form onSubmit={(e) => {
            e.preventDefault();

            const id = parseInt(addNodeId);

            if (isNaN(id)) {
                setError("Please provide a valid node id")
                return;
            }

            if (selectedNodes[id]) {
                setError("Node already selected");
                return;
            }

            setError(null);
            dispatch(thunks.getNodes(dbName, [id], { yield_edges: true }))
                .then(res => dispatch(actions.selectNode(Object.values(res)[0])));
        }}>
            <Box display="flex">
                <TextField
                    type="number"
                    id="node-id-field"
                    sx={{ p: 1 }}
                    size="small"
                    onChange={e => setAddNodeId(e.target.value)}
                />
                <Button type="submit">Add node</Button>
            </Box>
        </form>
    </BorderedContainerTitle>
}


export default function NodeFilterContainer({
    selectedNodeType,
    propertyValue
}) {
    const [tooManyNodes, setTooManyNodes] = React.useState(false);
    const [error, setError] = React.useState(null);
    const selectedNodes = useSelector(state => state.selectedNodes);
    const dbName = useSelector(state => state.dbName);
    const inspectedNode = useSelector(state => state.inspectedNode);
    const displayedNodeProperty = useSelector(state => state.displayedNodeProperty);
    const titleProps = { setError };
    const dispatch = useDispatch();

    const Content = ({ children }) => {
        return <BorderedContainer title={<Title {...titleProps} />}>
            {error && <Alert severity="error">{error}</Alert>}
            <NodeStack>{children}</NodeStack>
        </BorderedContainer >;
    }

    const { data, isFetching } = useQuery(
        ["list_nodes", dbName, selectedNodeType, displayedNodeProperty, propertyValue],
        React.useCallback(() => dispatch(thunks
            .fetchNodes(dbName, {
                ...selectedNodeType ? { node_type_name: selectedNodeType } : {},
                ...displayedNodeProperty ? { prop_name: displayedNodeProperty } : {},
                ...propertyValue ? { prop_value: propertyValue } : {},
                yield_edges: false,
            }))
            .then(res => {
                if (res.error) {
                    setTooManyNodes(true);
                    return {}
                }

                //dispatch(actions.cacheNodes(res));
                setTooManyNodes(false);
                return Object.fromEntries(res.map(n => [n.id, n]));
            })
            , [dbName, dispatch, displayedNodeProperty, propertyValue, selectedNodeType])
    )

    if (tooManyNodes) {
        return <Content><Box m={1}>Too many nodes requested</Box></Content>
    }

    if (isFetching) {
        return <Content><Box m={1}>Loading nodes <CircularProgress s={10} /></Box></Content>
    }

    const currentNodes = data || {};
    const currentNodeKeys = Object.keys(currentNodes);
    const filteredKeys = currentNodeKeys
        .filter(id => !(id in selectedNodes))
        .slice(0, 100);

    const filteredNodeCount = currentNodes.length - filteredKeys.length;
    const remainingNodeCount = currentNodes.length - filteredNodeCount - filteredKeys.length;

    return <BorderedContainer title={<Title {...titleProps} />}>
        {error && <Alert severity="error">{error}</Alert>}
        <NodeInspector
            open={inspectedNode !== null}
            onClose={() => dispatch(thunks.inspectNode(dbName, null))} />
        <Button onClick={() =>
            dispatch(thunks.getNodes(dbName, Object.keys(currentNodes), { yield_edges: true }))
                .then(res => dispatch(actions.selectNodes(Object.values(res))))
        }>Select all</Button>
        <NodeStack>
            {filteredKeys.map((id, i) => {
                return <NodeChip
                    node={currentNodes[id]}
                    key={i}
                    nodeId={id}
                />;
            })}
        </NodeStack>
        {remainingNodeCount > 0 &&
            <Box p={1}>... {remainingNodeCount} more nodes</Box>}
    </BorderedContainer >;
}

