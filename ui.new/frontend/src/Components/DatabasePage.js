import { AppContext } from './App'
import React from 'react'
import { Box, Button } from '@mui/material';
import axios from 'axios'
import { DBInspector } from './'


export default function ViewerPage() {
    const [dbLoading, setDbLoading] = React.useState(false);
    const [dbLoaded, setDbLoaded] = React.useState(false);
    const [fetchingDbLoaded, setFetchingDbLoaded] = React.useState(false);
    const context = React.useContext(AppContext)

    const contextRef = React.useRef(context);
    React.useEffect(() => {
        const c = contextRef.current;
        setDbLoading(false);
        setFetchingDbLoaded(true);
        c.setStatus("", true);
        axios
            .get('/api/is_db_loaded', {
                params: { db_name: context.currentDb }
            })
            .then(res => {
                setDbLoaded(res.data.loaded);
                setFetchingDbLoaded(false);
                c.setIdleStatus()
            })
            .catch(err => {
                console.log(err);
            });
    }, [contextRef, context.currentDb]);

    const dbLoadedRef = React.useRef(dbLoaded);
    React.useEffect(() => {
        const c = contextRef.current;
        if (dbLoading) {
            if (!dbLoadedRef.current) {
                axios.post("/api/load_database", {
                    db_name: c.currentDb
                }).then(_res => {
                    setDbLoading(false);
                    setDbLoaded(true);
                    c.setIdleStatus();
                });
            }
        }
    }, [dbLoading, dbLoadedRef]);

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
