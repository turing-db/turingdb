import Typography from '@mui/material/Typography'
import { AppContext } from './App'
import { useCallback, useContext, useEffect, useRef, useState } from 'react'
import { Box, Button, CircularProgress, Container } from '@mui/material';
import axios from 'axios'
import { DBInspector } from './'


export default function ViewerPage() {
    const [dbLoading, setDbLoading] = useState(false);
    const [dbLoaded, setDbLoaded] = useState(false);
    const [fetchingDbLoaded, setFetchingDbLoaded] = useState(false);
    const context = useContext(AppContext)

    const loadDatabase = () => {
        axios.post("/api/load_database", {
            db_name: context.currentDb
        }).then(_res => {
            setDbLoading(false);
            setDbLoaded(true);
            context.setIdleStatus();
        });
    }

    useEffect(() => {
        setDbLoading(false);
        setFetchingDbLoaded(true);
        context.setStatus("", true);
        axios
            .get('/api/is_db_loaded', {
                params: { db_name: context.currentDb }
            })
            .then(res => {
                setDbLoaded(res.data.loaded);
                setFetchingDbLoaded(false);
                context.setIdleStatus()
            })
            .catch(err => {
                console.log(err);
            });
    }, [context.currentDb]);

    useEffect(() => {
        if (dbLoading) {
            if (!dbLoaded) {
                loadDatabase();
            }
        }
    }, [dbLoading]);

    var content = <Box></Box>;

    if (!context.currentDb) {
        content = <Box>Select a database to start</Box>;
    }

    else if (!dbLoaded) {
        content = <Box sx={{ display: "flex" }}>
            <Button
                size="large"
                variant="outlined"
                sx={{
                    justifyContent: "center"
                }}
                onClick={() => {
                    setDbLoading(true);
                    context.setStatus("Loading database", true);
                }}
            >
                Load database
            </Button>
        </Box>;
    }
    else if (dbLoading || fetchingDbLoaded) {
    }
    else {
        content = <DBInspector />;
    }

    return content;
}
