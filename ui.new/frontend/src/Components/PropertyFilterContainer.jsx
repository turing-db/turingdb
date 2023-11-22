// Core
import React from "react";
import { useDispatch, useSelector } from "react-redux";

// @mui/material
import { Autocomplete, IconButton, TextField } from "@mui/material";

// @mui/icons-material
import SearchIcon from "@mui/icons-material/Search";

// Turing
import BorderedContainer from "src/Components/BorderedContainer";
import { BorderedContainerTitle } from "src/Components/BorderedContainer";
import { useNodePropertyTypesQuery } from "src/App/queries";
import * as actions from "src/App/actions";

const useProperty = ({ setPropertyValue }) => {
  const dispatch = useDispatch();
  const name = React.useRef(null);
  const value = React.useRef("");
  const [displayedValue, setDisplayedValue] = React.useState("");

  const search = React.useCallback(() => {
    setPropertyValue(value.current);
  }, [setPropertyValue]);

  const setName = React.useCallback(
    (v) => {
      name.current = v;
      dispatch(actions.setNodeLabel(v));
    },
    [dispatch]
  );

  const setValue = React.useCallback((v) => {
    value.current = v;
    setDisplayedValue(v);
  }, []);

  return {
    name,
    value,
    displayedValue,
    search,
    setName,
    setValue,
  };
};

export default function PropertyFilterContainer({ setPropertyValue }) {
  const dispatch = useDispatch();
  const { displayedValue, search, setName, setValue } = useProperty({
    setPropertyValue,
  });
  const nodeLabel = useSelector((state) => state.nodeLabel);
  const { data: rawNodePropertyTypes } = useNodePropertyTypesQuery();
  const nodePropertyTypes = React.useMemo(
    () => rawNodePropertyTypes || [],
    [rawNodePropertyTypes]
  );
  const namingProps = React.useMemo(
    () =>
      ["displayName", "label", "name", "Name", "NAME"].filter((p) =>
        nodePropertyTypes.includes(p)
      ),
    [nodePropertyTypes]
  );

  const defaultNodeProperty = React.useMemo(() => {
    if (namingProps[0]) return namingProps[0];

    const regexProps = nodePropertyTypes.map((p) =>
      p.match("/name|Name|NAME|label|Label|LABEL/")
    );
    return regexProps.length !== 0 ? regexProps[0] : nodePropertyTypes[0];
  }, [namingProps, nodePropertyTypes]);

  React.useEffect(() => {
    if (nodeLabel === null && defaultNodeProperty !== undefined) {
      dispatch(actions.setNodeLabel(defaultNodeProperty));
    }
  }, [defaultNodeProperty, dispatch, nodeLabel]);

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        search();
      }}>
      <BorderedContainer
        title={
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
              value={nodeLabel}
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
                  }}
                />
              )}
            />
            <TextField
              required
              id="outlined-controlled"
              label="Property value"
              value={displayedValue}
              onChange={(e) => setValue(e.target.value)}
              size="small"
            />
            <IconButton type="submit">
              <SearchIcon />
            </IconButton>
          </BorderedContainerTitle>
        }></BorderedContainer>
    </form>
  );
}
