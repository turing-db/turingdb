import axios from 'axios'
import React from 'react'
import { useSelector } from 'react-redux';
import { useQuery } from '@tanstack/react-query';
import { useTheme } from '@emotion/react';
import HubIcon from '@mui/icons-material/Hub';

import {
    Box,
    Button,
    CircularProgress,
    ListItem,
    ListItemIcon,
    Typography
} from "@mui/material";

import { Secondary } from './Span'
import NodeInspector from './NodeInspector';


const SideNodeInspector = (props) => {
    const theme = useTheme();
    const { open, menuExpanded } = props
    const inspectedNode = useSelector((state) => state.inspectedNode);
    const dbName = useSelector((state) => state.dbName);
    const [showFullInspector, setShowFullInspector] = React.useState(false);

    const listNodeProperties = React.useCallback(async () =>
        inspectedNode && await axios
            .post("/api/list_node_properties", {
                db_name: dbName,
                id: inspectedNode.id,
            })
            .then(res => {
                return res.data;
            })
            .catch(err => {
                console.log(err);
                return {};
            })
        , [dbName, inspectedNode])

    const queryKey = [
        "list_node_properties",
        inspectedNode?.id
    ];
    const { data, status } = useQuery(
        queryKey,
        listNodeProperties,
    );

    const properties = data || {};
    if (!inspectedNode) return <></>;

    return <Box
        m={1}
        p={1}
        {...menuExpanded && {
            border: 1,
            borderRadius: 1,
            borderColor: theme.palette.secondary.main
        }}
    >
        <ListItem
            key="NodeInspector"
            sx={{
                display: 'block',
                px: 0.5,
                mr: menuExpanded ? 3 : 0,
                justifyContent: open ? 'initial' : 'center',
            }}
        >
            <ListItemIcon
                sx={{
                    minHeight: 48,
                }}
            >
                <HubIcon sx={{ mr: 3, color: theme.palette.primary.main }} />
                <Typography sx={{ color: theme.palette.primary.main }}>Node {inspectedNode.id}</Typography>
            </ListItemIcon>
            {menuExpanded &&
                (inspectedNode & status === "loading"
                    ? <CircularProgress size={20} />
                    : <Box>
                        <Box overflow="auto">
                        {Object.keys(properties).map(pName =>
                            <Box
                                key={"box-key-" + pName}
                                display="flex"
                                alignItems="center"
                            >
                                <Typography
                                    variant="body2"
                                >
                                    <Secondary>{pName}</Secondary>: <span>{properties[pName].toString()}</span>
                                </Typography>
                            </Box>)}
                        </Box>
                        <Button onClick={() => setShowFullInspector(true)}>Inspect node</Button>
                        <NodeInspector open={showFullInspector} onClose={() => setShowFullInspector(false)}/>
                    </Box>
                )
            }
        </ListItem>
    </Box>
};

export default SideNodeInspector;
