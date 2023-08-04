import axios from 'axios'
import {
    Box,
    Chip,
    Button,
    CircularProgress,
    Grid,
    Typography,
    IconButton,
    Divider,
    TextField,
    Alert
} from '@mui/material'
import CachedIcon from '@mui/icons-material/Cached'
import React from 'react'
import { AppContext } from './App'
import { BorderedContainer, NodeStack } from './'
import { useTheme } from '@emotion/react';

export default function NodeFilterContainer({
    selectedNodeType
}) {
    const [loading, setLoading] = React.useState(false);
    const [nodes, setNodes] = React.useState([]);
    const [tooManyNodes, setTooManyNodes] = React.useState(false);
    const [error, setError] = React.useState(null);
    const addNodeId = React.useRef(null);
    const theme = useTheme()
    const context = React.useContext(AppContext);

    React.useEffect(() => {
        setLoading(true);
    }, [selectedNodeType]);

    const Content = ({ children }) => {
        return <BorderedContainer>
            <Box display="flex" alignItems="center">
                <Typography
                    variant="h6"
                    p={1}
                    pl={3}
                    color={theme.palette.primary.main}
                    bgcolor={theme.palette.background.paper}
                >
                    Nodes
                </Typography>
                <IconButton onClick={() => setLoading(true)}>
                    <CachedIcon />
                </IconButton>

                <form onSubmit={(e) => {
                    e.preventDefault();

                    const id = parseInt(addNodeId.current);
                    if (!id) {
                        setError("Please provide a node id")
                        return;
                    }

                    axios
                        .get("/api/list_nodes", {
                            params: {
                                db_name: context.currentDb,
                                ids: [id].reduce((f, s) => `${f},${s}`)
                            }
                        })
                        .then(res => {
                            if (res.data.error) {
                                setError("Unknown error")
                                return;
                            }
                            if (res.data.length === 0) {
                                setError("Node does not exist");
                                return;
                            }

                            const _node = res.data[0];
                            if (context.selectedNodes.has(_node)) {
                                setError("Node already selected");
                                return;
                            }
                            context.selectedNodes.add(_node);
                            addNodeId.current = null;
                            setError(null);

                        })
                        .catch(err => {
                            console.log(err);
                            setError("Unknown error");
                        })
                }}>
                    <Box display="flex" >
                        <TextField
                            type="number"
                            id="node-id-field"
                            sx={{ p: 1 }}
                            size="small"
                            onChange={(e) => { addNodeId.current = e.target.value }}
                        />
                        <Button type="submit">Add node</Button>
                    </Box>
                </form>
            </Box>
            <Divider />
            {error && <Alert severity="error">{error}</Alert>}
            <NodeStack>{children}</NodeStack>
        </BorderedContainer >;
    }

    if (loading) {
        axios
            .get("/api/list_nodes", {
                params: {
                    db_name: context.currentDb,
                    ...(selectedNodeType ? { node_type_name: selectedNodeType } : {})
                }
            })
            .then(res => {
                setLoading(false);
                if (res.data.error) {
                    setTooManyNodes(true);
                    return;
                }
                setNodes(res.data);
                setTooManyNodes(false);
            })
            .catch(err => {
                setLoading(false);
                setNodes([]);
                console.log("Error: ", err);
            });

        return <Content>
            <Box m={1}>Loading nodes</Box><Box><CircularProgress size={20} /></Box>
        </Content>
    }

    if (tooManyNodes) {
        return <Content><Box m={1}>Too many nodes requested</Box></Content>
    }

    const onNodeSelect = id =>
        context.selectedNodes.add(nodes.filter(n => n.id === id)[0]);

    const GridItem = ({ children }) => {
        const chipProps = {
            label: children,
            variant: "outlined",
            onClick: () => onNodeSelect(children),
        };
        return <Grid item p={0.5}><Chip{...chipProps}></Chip></Grid>;
    }

    const filtered = nodes.filter(n => !context.selectedNodes.has(n));
    const slice = filtered.slice(0, 100);
    const filteredNodeCount = nodes.length - filtered.length;
    const remainingNodeCount = nodes.length - filteredNodeCount - slice.length;

    return <Content>
        {slice.map(({ id }, _i) => {
            return <GridItem key={_i}>{id}</GridItem>;
        })}
        {remainingNodeCount > 0 &&
            <Box p={1}>... {remainingNodeCount} more nodes</Box>}
    </Content>
}
