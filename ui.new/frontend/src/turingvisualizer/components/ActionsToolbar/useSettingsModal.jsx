import React from "react";
import {
  Tooltip,
  Checkbox,
  Slider,
  FormGroup,
} from "@blueprintjs/core";

import { useVisualizerContext, useCanvasTrigger } from "../../";
import { INIT_EDGE_VAL } from "../../reducers/layouts";
import { useDialog } from "src/turingvisualizer/dialogs";

const LENGTH_RATIO = 0.1;

const useSettingsModal = () => {
  const vis = useVisualizerContext();
  const [filters, setFilters] = React.useState(vis.state().filters);
  const [edgeLengthVal, setEdgeLengthVal] = React.useState(
    INIT_EDGE_VAL * LENGTH_RATIO
  );

  const FilterCheckbox = (props) => {
    const vis = useVisualizerContext();

    return (
      <Tooltip content={props.tooltip || props.label} usePortal={false}>
        <Checkbox
          label={props.label}
          checked={props.propValue}
          onChange={(e) => {
            vis.callbacks().setFilters({
              ...vis.state().filters,
              [props.propName]: e.target.checked,
            });
          }}
        />
      </Tooltip>
    );
  };

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

  useDialog({
    name: "show-settings",
    title: "Graph settings",
    content: () => {
      return <div style={{
        display: "flex",
        flexDirection: "column",
        padding: 20
      }}>
        <FormGroup label="Edge length">
          <Slider
            min={1}
            labelValues={[1, 50, 100]}
            max={100}
            initialValue={edgeLengthVal}
            value={edgeLengthVal}
            onChange={(v) => {
              setEdgeLengthVal(v);
            }}
            onRelease={(v) =>
              vis.callbacks().setDefaultCyLayout({
                ...vis.state().layouts.definitions[0],
                edgeLengthVal: v / LENGTH_RATIO,
              })
            }
          />
        </FormGroup>
        <div style={{ display: "flex", flexDirection: "column" }}>
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
        </div>
      </div>;
    },
  });
};

export default useSettingsModal;
