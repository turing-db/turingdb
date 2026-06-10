#pragma once

namespace db {

class Graph;

// Miniature Reactome graph (41 nodes, 60 edges) modelled after the real Reactome
// Knowledgebase schema: TopLevelPathway / Pathway / ReactionLikeEvent hierarchy,
// Complex / PhysicalEntity / ReferenceGeneProduct, Species and Compartment, with
// hasEvent / hasComponent / species / input / output / compartment edge types. Used as a
// realistic, multi-label, multi-property, multi-edge-type fixture for dump/load and query
// tests.
class ReactomeSampleGraph {
public:
    static void create(Graph* graph);

    ReactomeSampleGraph() = delete;
};

}
