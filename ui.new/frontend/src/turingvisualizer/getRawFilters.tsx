
export type Filters = {
  hideCompartments: boolean,
  hideSpecies: boolean,
  hidePublications: boolean,
  hideDatabaseReferences: boolean,
  showOnlyHomoSapiens: boolean
};

export const getRawFilters = (filters: Filters) => {
  const nodePropertyFilterOut = [
    ...(filters.hideCompartments ? [["schemaClass", "Compartment"]] : []),

    ...(filters.hideSpecies ? [["schemaClass", "Species"]] : []),

    ...(filters.hidePublications
      ? [
          ["schemaClass", "InstanceEdit"],
          ["schemaClass", "ReviewStatus"],
          ["schemaClass", "LiteratureReference"],
        ]
      : []),

    ...(filters.hideDatabaseReferences
      ? [
          ["schemaClass", "ReferenceGeneProduct"],
          ["schemaClass", "ReferenceDatabase"],
          ["schemaClass", "ReferenceDNASequence"],
          ["schemaClass", "ReferenceRNASequence"],
        ]
      : []),
  ];

  const nodePropertyFilterIn = [
    ...(filters.showOnlyHomoSapiens ? [["speciesName", "Homo sapiens"]] : []),
  ];

  return {
    nodePropertyFilterOut,
    nodePropertyFilterIn,
  };
};

