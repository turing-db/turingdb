// Core
import React from "react";
import axios from "axios";
import { useSelector } from "react-redux";

// @mui/material
import { Autocomplete, CircularProgress, TextField } from "@mui/material";

import BorderedContainer from "src/Components/BorderedContainer";
import { BorderedContainerTitle } from "src/Components/BorderedContainer";
import { useQuery } from "src/App/queries";

export default function NodeTypeFilterContainer({ selected, setSelected }) {
  const dbName = useSelector((state) => state.dbName);
  const [enabled, setEnabled] = React.useState(false);
  const { data, isFetching } = useQuery(
    ["list_node_types", dbName],
    () =>
      axios
        .post("/api/list_node_types", { db_name: dbName })
        .then((res) => res.data)
        .catch((err) => {
          console.log(err);
          return [];
        }) || [],
    { enabled }
  );
  const nodeTypes = data || [];

  return (
    <BorderedContainer
      title={
        <BorderedContainerTitle title="NodeType" noDivider>
          <Autocomplete
            id="node-type-filter"
            onOpen={() => setEnabled(true)}
            onChange={(_e, nt) => setSelected(nt)}
            value={selected}
            options={nodeTypes}
            sx={{ width: "100%", p: 1 }}
            size="small"
            renderInput={(params) => (
              <TextField
                {...params}
                label="Node type"
                InputProps={{
                  ...params.InputProps,
                  endAdornment: (
                    <React.Fragment>
                      {isFetching ? (
                        <CircularProgress color="inherit" size={20} />
                      ) : null}
                      {params.InputProps.endAdornment}
                    </React.Fragment>
                  ),
                }}
              />
            )}
          />
        </BorderedContainerTitle>
      }></BorderedContainer>
  );
}
