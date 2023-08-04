import { Box, Chip, Grid, Typography, Divider, IconButton } from '@mui/material'
import { AppContext } from './App'
import DeleteIcon from '@mui/icons-material/Delete'
import { BorderedContainer, NodeStack } from './'
import { useTheme } from '@emotion/react';
import React from 'react'

export default function SelectedNodesContainer() {
    const context = React.useContext(AppContext);
    const theme = useTheme()

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
                    Selected nodes
                </Typography>
                {context.selectedNodes.ids.size !== 0 &&
                    <IconButton onClick={() => {
                        context.selectedNodes.clear();
                    }}>
                        <DeleteIcon />
                    </IconButton>}
            </Box>
            <Divider />

            <NodeStack>{children}</NodeStack>
        </BorderedContainer >;
    }

    const onDelete = (id) => {
        context.selectedNodes.removeFromId(id);
    };

    const GridItem = ({ children }) => {
        const chipProps = {
            label: children,
            variant: "outlined",
            onDelete: _ => onDelete(children)
        };
        return <Grid item p={0.5}><Chip{...chipProps}></Chip></Grid>;
    };

    var rows = [];
    context.selectedNodes.ids.forEach(
        (id, i) => rows.push(<GridItem key={i}>{id}</GridItem>)
    )

    return <Content>
        {rows}
    </Content>
}
