import { Autocomplete, IconButton, TextField } from '@mui/material';
import React from 'react';
import BorderedContainer from './BorderedContainer';
import SearchIcon from '@mui/icons-material/Search';
import { BorderedContainerTitle } from './BorderedContainer';
import { useNodePropertyTypesQuery } from '../queries';
import { useDispatch, useSelector } from 'react-redux';
import * as actions from '../actions';

const useProperty = ({ setPropertyValue }) => {
    const dispatch = useDispatch();
    const name = React.useRef(null);
    const value = React.useRef("");
    const [displayedValue, setDisplayedValue] = React.useState("");

    const search = React.useCallback(() => {
        setPropertyValue(value.current);
    }, [setPropertyValue]);

    const setName = React.useCallback((v) => {
        name.current = v;
        dispatch(actions.selectDisplayedProperty(v));
    }, [dispatch]);

    const setValue = React.useCallback((v) => {
        value.current = v;
        setDisplayedValue(v);
    }, []);

    return {
        name, value, displayedValue,
        search,
        setName, setValue
    };
}

export default function PropertyFilterContainer({
    setPropertyValue,
}) {
    const dispatch = useDispatch();
    const {
        displayedValue,
        search,
        setName, setValue
    } = useProperty({ setPropertyValue });
    const displayedNodeProperty = useSelector(state => state.displayedNodeProperty);
    const { data: rawNodePropertyTypes } = useNodePropertyTypesQuery();
    const nodePropertyTypes = React.useMemo(() => rawNodePropertyTypes || [], [rawNodePropertyTypes]);
    const namingProps = React.useMemo(() =>
        ["displayName", "label", "name", "Name", "NAME"]
            .filter(p => nodePropertyTypes.includes(p))
        , [nodePropertyTypes]);

    const defaultNodeProperty = React.useMemo(() => {
        if (namingProps[0]) return namingProps[0];

        const regexProps = nodePropertyTypes.map(p => p.match("/name|Name|NAME|label|Label|LABEL/"));
        return regexProps.length !== 0
            ? regexProps[0]
            : nodePropertyTypes[0];
    }, [namingProps, nodePropertyTypes])

    React.useEffect(() => {
        if (displayedNodeProperty === null && defaultNodeProperty !== undefined) {
            dispatch(actions.selectDisplayedProperty(defaultNodeProperty))
        }

    }, [defaultNodeProperty, dispatch, displayedNodeProperty]);

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
                    onChange={(e, v) => {
                        setName(v);
                        if (!v) {
                            setValue("");
                            e.currentTarget.form.requestSubmit();
                        }
                    }}
                    value={displayedNodeProperty}
                    options={nodePropertyTypes}
                    sx={{ width: "50%", m: 1 }}
                    size="small"
                    autoSelect
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            required
                            label="Property"
                            InputProps={{
                                ...params.InputProps,
                                endAdornment: (
                                    <React.Fragment>
                                        {params.InputProps.endAdornment}
                                    </React.Fragment>
                                ),
                            }} />
                    )}
                />
                <TextField
                    required
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

