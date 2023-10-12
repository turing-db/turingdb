import React from 'react'
import axios from 'axios'
import { Box, Button, CircularProgress, Typography } from '@mui/material'
import DBInspector from './DBInspector'
import { Secondary } from './Span'
import { useQuery } from '../queries'
import { useSelector } from 'react-redux'

function DBLoader() {
    const dbName = useSelector((state) => state.dbName);
    const [isLoading, setIsLoading] = React.useState(false);
    const { data: loaded, refetch } = useQuery(
        ["is_db_loaded", dbName],
        React.useCallback(async () => {
            return dbName && await axios
                .get('/api/is_db_loaded', { params: { db_name: dbName } })
                .then(res => res.data.error ? false : res.data.loaded)
                .catch(err => { console.log(err); return false })
        }, [dbName]),
        { staleTime: 0 }
    );

    React.useEffect(() => { refetch() }, [dbName, refetch]);

    const loadDatabase = React.useCallback(() => {
        setIsLoading(true);
        axios
            .post('/api/load_database', { db_name: dbName })
            .then(() => { setIsLoading(false); refetch(); });
    }, [dbName, refetch]);

    if (!dbName)
        return;

    return (
        <Box>
            {isLoading &&
                <Box display="flex" justifyContent="center" alignItems="center">
                    <Typography p={2} variant="h4">Loading <Secondary variant="h4">{dbName}</Secondary></Typography>
                    <CircularProgress size={20} />
                </Box>}
            {loaded && <DBInspector />}

            {!loaded && <Box display="flex" flexDirection="column" alignItems="center">
                <Typography p={2}>Database <Secondary>{dbName}</Secondary> not loaded.</Typography>
                <Button variant="contained" color="primary" onClick={loadDatabase}>
                    Load Database
                </Button>
            </Box>}
        </Box>
    );
}

export default function DatabasePage() {
    return <DBLoader />;
}
