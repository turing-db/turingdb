import axios from 'axios'
import { Autocomplete, Box, Chip, CircularProgress, Grid, TextField, Typography } from '@mui/material'
import React from 'react'
import { AppContext } from './App'
import { BorderedContainer } from './'
import { useTheme } from '@emotion/react';

export default function NodeTypeFilterContainer({ selected, setSelected }) {
    const [loading, setLoading] = React.useState(true);
    const [nodeTypes, setNodeTypes] = React.useState([]);
    const context = React.useContext(AppContext);
    const theme = useTheme();

    const Content = () => {
        return <BorderedContainer>
            <Box display="flex" alignItems="center">
                <Typography
                    variant="h6"
                    p={1}
                    pl={3}
                    color={theme.palette.primary.main}
                    bgcolor={theme.palette.background.paper}
                >
                    NodeType
                </Typography>
                <Autocomplete
                    id="node-type-filter"
                    onChange={(_e, nt) => {
                        setSelected(nt);
                    }}
                    value={selected}
                    options={nodeTypes}
                    sx={{ width: "100%", p: 1 }}
                    size="small"
                    renderInput={(params) => <TextField {...params} />}
                />
            </Box>
        </BorderedContainer >;
    }

    if (loading) {
        axios
            .get("/api/list_node_types", {
                params: {
                    db_name: context.currentDb
                }
            })
            .then(res => {
                setLoading(false);
                setNodeTypes(res.data)
            })
            .catch(err => {
                setLoading(false);
                console.log(err);
            })
        return <Content><CircularProgress size={20} /></Content>
    }

    const onNodeTypeFilter = (nodeTypeName) => {
        setSelected(nodeTypeName)
    }

    const onDelete = () => {
        setSelected(null);
    }

    const GridItem = ({ selected, children }) => {
        const chipProps = {
            label: children,
            variant: selected ? "filled" : "outlined",
            id: children,
            onClick: () => onNodeTypeFilter(children),
            ...(selected ? { onDelete: onDelete } : {}),
        };
        return <Grid item p={0.5}><Chip{...chipProps}></Chip></Grid>;
    }

    return <Content>
        {nodeTypes.map((ntName, _i) => {
            return <GridItem
                key={_i}
                selected={ntName === selected}
            >
                {ntName}
            </GridItem>;
        })}
    </Content>
}
