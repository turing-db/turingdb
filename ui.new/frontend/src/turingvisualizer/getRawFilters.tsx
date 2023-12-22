import { Property } from "./types";

export type Filters = {
  hideCompartments?: boolean;
  hideSpecies?: boolean;
  hidePublications?: boolean;
  hideDatabaseReferences?: boolean;
  showOnlyHomoSapiens?: boolean;
};

export function getRawFilters(filters: Filters) {
  let nodePropertyFilterOut: Property[] = [];
  let nodePropertyFilterIn: Property[] = [];

  if (filters.hideCompartments) {
    nodePropertyFilterOut.push(["schemaClass", "Compartment"]);
  }
  if (filters.hideSpecies) {
    nodePropertyFilterOut.push(["schemaClass", "Species"]);
  }

  if (filters.hidePublications) {
    nodePropertyFilterOut.push(["schemaClass", "InstanceEdit"]);
    nodePropertyFilterOut.push(["schemaClass", "ReviewStatus"]);
    nodePropertyFilterOut.push(["schemaClass", "LiteratureReference"]);
  }

  if (filters.hideDatabaseReferences) {
    nodePropertyFilterOut.push(["schemaClass", "ReferenceGeneProduct"]);
    nodePropertyFilterOut.push(["schemaClass", "ReferenceDatabase"]);
    nodePropertyFilterOut.push(["schemaClass", "ReferenceDNASequence"]);
    nodePropertyFilterOut.push(["schemaClass", "ReferenceRNASequence"]);
  }

  if (filters.showOnlyHomoSapiens) {
    nodePropertyFilterIn.push(["speciesName", "Homo sapiens"]);
  }

  return {
    nodePropertyFilterOut,
    nodePropertyFilterIn,
  };
}
