import { Autocomplete, TextField } from '@mui/material'
import React, { useContext, useState } from 'react'
import { AppContext } from './App'
import axios from 'axios'

export default function DBSelector() {
    const { currentDb, setCurrentDb } = useContext(AppContext);
    const [availableDbs, setAvailableDbs] = useState([]);

    // See https://mui.com/material-ui/react-autocomplete/ for an async
    // version to wait for the server to respond (Load on open section)
    return <Autocomplete
        disablePortal
        id="db_selector"
        onChange={(_e, newDb) => {
            setCurrentDb(newDb);
        }}
        onOpen={(_e) => {
            axios.get("/api/list_available_databases")
                .then(res => { setAvailableDbs(res.data.db_names) })
                .catch(err => { console.log(err) });
        }}
        options={availableDbs}
        value={currentDb}
        sx={{ width: 300 }}
        size="small"
        renderInput={(params) => <TextField {...params} label="Database" />}
    />;
}

