import { Autocomplete, TextField } from '@mui/material';
import { useEffect, useState } from 'react'

const DBSelector = (props) => {
    return (
        <div className="flex justify-center text-secondary bg-primary p-4">
            <Autocomplete
                disablePortal
                id="db_selector"
                onChange={(event, newValue) => {
                    props.setSelectedDbName(newValue)
                }}
                options={props.available}
                sx={{ width: 300 }}
                renderInput={(params) => <TextField {...params} label="Select a database" />}
            />
        </div>
    );
};

export default DBSelector;
