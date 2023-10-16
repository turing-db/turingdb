import React from 'react'
import axios from 'axios'

import { Autocomplete, CircularProgress, TextField } from '@mui/material'
import { useDispatch, useSelector } from 'react-redux';
import { setDbName } from '../actions';
import { useQuery } from '../queries'

export default function DBSelector() {
    const dbName = useSelector((state) => state.dbName);
    const dispatch = useDispatch();
    const [enabled, setEnabled] = React.useState(false);

    const { data: availableDbs, isFetching } = useQuery(
        ["list_available_databases"],
        React.useCallback(async () =>
            await axios.get("/api/list_available_databases")
                .then(res => res.data.db_names)
                .catch(err => { console.log(err); return [] })
            , []),
        { enabled });

    // See https://mui.com/material-ui/react-autocomplete/ for an async
    // version to wait for the server to respond (Load on open section)
    return <Autocomplete
        disablePortal
        blurOnSelect
        id="db_selector"
        onOpen={() => setEnabled(true)}
        onChange={(_e, newDb) => newDb && dispatch(setDbName(newDb))}
        autoSelect
        autoHighlight
        loading={isFetching}
        options={availableDbs || []}
        value={dbName}
        sx={{ width: 300 }}
        size="small"
        renderInput={(params) => (
            <TextField
                {...params}
                label="Database"
                InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                        <React.Fragment>
                            {isFetching ? <CircularProgress color="inherit" size={20} /> : null}
                            {params.InputProps.endAdornment}
                        </React.Fragment>
                    ),
                }} />
        )}
    />;
}

