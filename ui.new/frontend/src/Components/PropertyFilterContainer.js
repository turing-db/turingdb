import axios from 'axios'
import { Autocomplete, CircularProgress, IconButton, TextField } from '@mui/material'
import React from 'react'
import { BorderedContainer } from './'
import SearchIcon from '@mui/icons-material/Search'
import { BorderedContainerTitle } from './BorderedContainer'
import { useQuery } from '../App/queries'
import { useSelector } from 'react-redux'

const useProperty = ({ setPropertyName, setPropertyValue }) => {
    const name = React.useRef(null);
    const value = React.useRef("");
    const [displayedName, setDisplayedName] = React.useState(null);
    const [displayedValue, setDisplayedValue] = React.useState("");

    const search = React.useCallback(() => {
        setPropertyName(name.current);
        setPropertyValue(value.current);
    }, [setPropertyName, setPropertyValue]);

    const setName = React.useCallback((v) => {
        name.current = v;
        setDisplayedName(v);
    }, []);

    const setValue = React.useCallback((v) => {
        value.current = v;
        setDisplayedValue(v);
    }, []);

    return {
        name, value,
        displayedName, displayedValue,
        search,
        setName, setValue
    };
}

export default function PropertyFilterContainer({
    setPropertyName,
    setPropertyValue,
}) {
    const [enabled, setEnabled] = React.useState(false);
    const dbName = useSelector((state) => state.dbName);
    const {
        displayedValue,
        search,
        setName, setValue
    } = useProperty({ setPropertyName, setPropertyValue });

    const { data, isFetching } = useQuery(
        ["list_string_property_types", dbName],
        React.useCallback(() => axios
            .post("/api/list_string_property_types", { db_name: dbName })
            .then(res => res.data)
            .catch(err => { console.log(err); return []; })
            , [dbName]),
        { enabled }
    )
    const propertyNames = data || [];

    return <form onSubmit={(e) => {
        e.preventDefault();
        search();
    }}
    >
        <BorderedContainer title={
            <BorderedContainerTitle title="Property" noDivider>
                <Autocomplete
                    id="property-name-filter"
                    blurOnSelect
                    onOpen={() => setEnabled(true)}
                    onChange={(e, v) => {
                        setName(v);
                        if (!v) {
                            setValue("");
                            e.currentTarget.form.requestSubmit();
                        }
                    }}
                    options={propertyNames}
                    sx={{ width: "50%", m: 1 }}
                    size="small"
                    autoSelect
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="Property"
                            InputProps={{
                                ...params.InputProps,
                                endAdornment: (
                                    <React.Fragment>
                                        {isFetching
                                            ? <CircularProgress color="inherit" size={20} />
                                            : null}
                                        {params.InputProps.endAdornment}
                                    </React.Fragment>
                                ),
                            }} />
                    )}
                />
                <TextField
                    id="outlined-controlled"
                    label="Property value"
                    value={displayedValue}
                    onChange={e => setValue(e.target.value)}
                    size="small"
                />
                <IconButton type="submit"><SearchIcon /></IconButton>
            </BorderedContainerTitle>}>
        </BorderedContainer >
    </form>;
}
