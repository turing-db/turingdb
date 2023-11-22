import React from "react";
import {
  Modal,
  Box,
  Typography,
  Slider,
  Tooltip,
  FormControlLabel,
  Checkbox,
  FormGroup,
} from "@mui/material";

import { useVisualizerContext, useCanvasTrigger } from "../../";
import { INIT_EDGE_VAL } from "../../reducers/layouts";

const LENGTH_RATIO = 0.1;

const FilterCheckbox = (props) => {
  const vis = useVisualizerContext();

  return (
    <Tooltip title={props.tooltip}>
      <FormControlLabel
        control={
          <Checkbox
            checked={props.propValue}
            onChange={(e) => {
              vis.callbacks().setFilters({
                ...vis.state().filters,
                [props.propName]: e.target.checked,
              });
            }}
          />
        }
        label={props.label}
      />
    </Tooltip>
  );
};

const SettingsModal = (props) => {
  const vis = useVisualizerContext();
  const [filters, setFilters] = React.useState({});
  const [edgeLengthVal, setEdgeLengthVal] = React.useState(
    INIT_EDGE_VAL * LENGTH_RATIO
  );

  useCanvasTrigger({
    category: "filters",
    name: "settingsModal-setFilters",
    callback: () => setFilters({ ...vis.state().filters }),
  });

  useCanvasTrigger({
    category: "layouts",
    name: "settingsModal-setEdgeLengthVal",
    callback: () =>
      setEdgeLengthVal(
        vis.state().layouts.definitions[0].edgeLengthVal * LENGTH_RATIO
      ),
  });

  const onClose = () => props.setOpen(false);

  const style = {
    position: "absolute",
    top: "50%",
    left: "50%",
    transform: "translate(-50%, -50%)",
    width: 400,
    bgcolor: "background.paper",
    border: "2px solid #000",
    boxShadow: 24,
    p: 4,
  };

  return (
    <Modal open={props.open} onClose={onClose}>
      <Box sx={style}>
        <Typography gutterBottom>Edge length</Typography>
        <Slider
          valueLabelDisplay="auto"
          aria-label="Edge length"
          min={1}
          max={100}
          value={edgeLengthVal}
          onChange={(_e, v) =>
            vis.callbacks().setDefaultCyLayout({
              ...vis.state().layouts.definitions[0],
              edgeLengthVal: v / LENGTH_RATIO,
            })
          }
        />

        <FormGroup>
          <FilterCheckbox
            label="Hide publications"
            propValue={filters.hidePublications}
            propName="hidePublications"
          />
          <FilterCheckbox
            label="Hide compartments"
            propValue={filters.hideCompartments}
            propName="hideCompartments"
            tooltip='Hide compartments such as "extracellular region" which can significantly complicate the visualization'
          />
          <FilterCheckbox
            label="Hide species"
            propValue={filters.hideSpecies}
            propName="hideSpecies"
            tooltip='Hide species nodes such as "Homo sapiens" which can significantly complicate the visualization'
          />
          <FilterCheckbox
            label="Hide database references"
            propValue={filters.hideDatabaseReferences}
            propName="hideDatabaseReferences"
          />
          <FilterCheckbox
            label="Homo sapiens only"
            propValue={filters.showOnlyHomoSapiens}
            propName="showOnlyHomoSapiens"
          />
        </FormGroup>
      </Box>
    </Modal>
  );
};

export default SettingsModal;
