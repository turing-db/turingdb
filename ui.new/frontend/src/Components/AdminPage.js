import '@alenaksu/json-viewer';
import React from 'react';
import axios from 'axios'
import { useQuery } from '../App/queries';
import { Box, Button, CircularProgress } from '@mui/material'
import { useSelector } from 'react-redux';

const AdminPage = () => {
    const selectedNodes = useSelector(state => state.selectedNodes);
    const nodeIds = Object.keys(selectedNodes);

    const { data, isFetching, refetch } = useQuery(
        ["get_admin_data", nodeIds],

        React.useCallback(async () =>
            await axios
                .post("/api/viewer/init", {
                    db_name: "pole",
                    node_ids: nodeIds
                })
                .then(res => res.data)
            , [nodeIds]),

        {
            refetchOnMount: true,
            refetchOnWindowFocus: true,
        });

    const Component = React.useCallback(() => isFetching
        ? <CircularProgress />

        : <json-viewer>
            {JSON.stringify({ data })}
        </json-viewer>

        , [data, isFetching]);

    return <Box>
        <Button onClick={() => refetch()}>Refresh</Button>
        <Component />
    </Box>;
}

export default AdminPage;

