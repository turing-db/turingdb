import React from "react";

import { Tooltip, Checkbox } from "@blueprintjs/core";
import { useVisualizerContext } from "../../context";

function useFilters() {
  const vis = useVisualizerContext();
  const filtersState = React.useState(vis.state()!.filters);

  React.useEffect(() => {
    Object.keys(filtersState[0]).forEach((key) => {
      if (filtersState[0][key] !== vis.state()!.filters[key]) {
        vis.callbacks()!.setFilters({
          ...vis.state()!.filters,
          ...filtersState[0],
        });
        return;
      }
    });
  }, [vis, filtersState]);

  return filtersState;
}

export default function NodeFilters() {
  const [filters, setFilters] = useFilters();

  return (
    <div className="flex flex-col">
      <Tooltip content="Hide publications">
        <Checkbox
          label="Hide publications"
          checked={filters.hidePublications}
          onChange={(e) => {
            setFilters({
              ...filters,
              hidePublications: e.target.checked,
            });
          }}
        />
      </Tooltip>

      <Tooltip content='Hide compartments such as "extracellular region"'>
        <Checkbox
          label="Hide compartments"
          checked={filters.hideCompartments}
          onChange={(e) => {
            setFilters({
              ...filters,
              hideCompartments: e.target.checked,
            });
          }}
        />
      </Tooltip>
      <Tooltip content='Hide species nodes such as "Homo sapiens"'>
        <Checkbox
          label="Hide species"
          checked={filters.hideSpecies}
          onChange={(e) => {
            setFilters({
              ...filters,
              hideSpecies: e.target.checked,
            });
          }}
        />
      </Tooltip>
      <Tooltip content="Hide database references">
        <Checkbox
          label="Hide database references"
          checked={filters.hideDatabaseReferences}
          onChange={(e) => {
            setFilters({
              ...filters,
              hideDatabaseReferences: e.target.checked,
            });
          }}
        />
      </Tooltip>
      <Tooltip content="Show Homo sapiens only nodes">
        <Checkbox
          label="Homo sapiens only"
          checked={filters.showOnlyHomoSapiens}
          onChange={(e) => {
            setFilters({
              ...filters,
              showOnlyHomoSapiens: e.target.checked,
            });
          }}
        />
      </Tooltip>
    </div>
  );
}
