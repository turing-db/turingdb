import { Tooltip, Checkbox } from "@blueprintjs/core";

export default function NodeFilters({filters, setFilters}) {
  return (
    <div className="flex flex-col w-max">
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
