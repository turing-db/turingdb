#include "ReactomeSampleGraph.h"

#include "Graph.h"
#include "metadata/PropertyType.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;

// =============================================================================
// Miniature Reactome graph
//
// Modelled after the real Reactome Knowledgebase schema:
//   - TopLevelPathway / Pathway / ReactionLikeEvent hierarchy
//   - Complex / PhysicalEntity / ReferenceGeneProduct
//   - Species, Compartment
//   - Edge types: hasEvent, hasComponent, species, input, output, compartment
//
// Biological focus areas:
//   - APOE-4 lipid metabolism (Alzheimer pathway)
//   - Autophagy (macro-autophagy initiation)
//   - EGFR signalling (receptor tyrosine kinase)
// =============================================================================

void ReactomeSampleGraph::create(Graph* graph) {
    JobSystem jobSystem;
    jobSystem.init();

    GraphWriter writer(graph, &jobSystem);
    writer.setName("reactome");

    // =================================================================
    // Species
    // =================================================================
    auto homoSapiens = writer.addNode({"Species"});
    writer.addNodeProperty<types::String>(homoSapiens, "displayName", "Homo sapiens");
    writer.addNodeProperty<types::Int64>(homoSapiens, "dbId", 48887);

    auto musMusculus = writer.addNode({"Species"});
    writer.addNodeProperty<types::String>(musMusculus, "displayName", "Mus musculus");
    writer.addNodeProperty<types::Int64>(musMusculus, "dbId", 48892);

    // =================================================================
    // Compartments
    // =================================================================
    auto cytosol = writer.addNode({"Compartment"});
    writer.addNodeProperty<types::String>(cytosol, "displayName", "cytosol");
    writer.addNodeProperty<types::Int64>(cytosol, "dbId", 70101);

    auto extracellular = writer.addNode({"Compartment"});
    writer.addNodeProperty<types::String>(extracellular, "displayName", "extracellular region");
    writer.addNodeProperty<types::Int64>(extracellular, "dbId", 70106);

    auto plasmaMembrane = writer.addNode({"Compartment"});
    writer.addNodeProperty<types::String>(plasmaMembrane, "displayName", "plasma membrane");
    writer.addNodeProperty<types::Int64>(plasmaMembrane, "dbId", 70110);

    auto endosome = writer.addNode({"Compartment"});
    writer.addNodeProperty<types::String>(endosome, "displayName", "endosome membrane");
    writer.addNodeProperty<types::Int64>(endosome, "dbId", 70115);

    // =================================================================
    // TopLevelPathways
    // =================================================================
    auto tlpSignal = writer.addNode({"TopLevelPathway", "Pathway"});
    writer.addNodeProperty<types::String>(
        tlpSignal, "displayName", "Signal Transduction");
    writer.addNodeProperty<types::String>(tlpSignal, "stId", "R-HSA-162582");
    writer.addNodeProperty<types::Int64>(tlpSignal, "dbId", 162582);

    auto tlpMetabolism = writer.addNode({"TopLevelPathway", "Pathway"});
    writer.addNodeProperty<types::String>(
        tlpMetabolism, "displayName", "Metabolism of lipids");
    writer.addNodeProperty<types::String>(tlpMetabolism, "stId", "R-HSA-556833");
    writer.addNodeProperty<types::Int64>(tlpMetabolism, "dbId", 556833);

    auto tlpAutophagy = writer.addNode({"TopLevelPathway", "Pathway"});
    writer.addNodeProperty<types::String>(
        tlpAutophagy, "displayName", "Autophagy");
    writer.addNodeProperty<types::String>(tlpAutophagy, "stId", "R-HSA-9612973");
    writer.addNodeProperty<types::Int64>(tlpAutophagy, "dbId", 9612973);

    // =================================================================
    // Sub-pathways
    // =================================================================
    auto pwEGFR = writer.addNode({"Pathway"});
    writer.addNodeProperty<types::String>(
        pwEGFR, "displayName", "Signaling by EGFR");
    writer.addNodeProperty<types::String>(pwEGFR, "stId", "R-HSA-177929");
    writer.addNodeProperty<types::Int64>(pwEGFR, "dbId", 177929);

    auto pwAPOE = writer.addNode({"Pathway"});
    writer.addNodeProperty<types::String>(
        pwAPOE, "displayName", "APOE4-mediated lipid transport");
    writer.addNodeProperty<types::String>(pwAPOE, "stId", "R-HSA-8866910");
    writer.addNodeProperty<types::Int64>(pwAPOE, "dbId", 8866910);

    auto pwMacroAutophagy = writer.addNode({"Pathway"});
    writer.addNodeProperty<types::String>(
        pwMacroAutophagy, "displayName", "Macro-autophagy initiation");
    writer.addNodeProperty<types::String>(pwMacroAutophagy, "stId", "R-HSA-1632852");
    writer.addNodeProperty<types::Int64>(pwMacroAutophagy, "dbId", 1632852);

    auto pwEGFRDownreg = writer.addNode({"Pathway"});
    writer.addNodeProperty<types::String>(
        pwEGFRDownreg, "displayName", "EGFR downregulation");
    writer.addNodeProperty<types::String>(pwEGFRDownreg, "stId", "R-HSA-182971");
    writer.addNodeProperty<types::Int64>(pwEGFRDownreg, "dbId", 182971);

    // Shared pathway: lipid transport appears under both metabolism and signal
    auto pwLipidTransport = writer.addNode({"Pathway"});
    writer.addNodeProperty<types::String>(
        pwLipidTransport, "displayName", "Lipid particle transport");
    writer.addNodeProperty<types::String>(pwLipidTransport, "stId", "R-HSA-8866423");
    writer.addNodeProperty<types::Int64>(pwLipidTransport, "dbId", 8866423);

    // =================================================================
    // ReactionLikeEvents
    // =================================================================
    auto rxnEGFRBinding = writer.addNode({"ReactionLikeEvent"});
    writer.addNodeProperty<types::String>(
        rxnEGFRBinding, "displayName", "EGF binds EGFR");
    writer.addNodeProperty<types::String>(rxnEGFRBinding, "stId", "R-HSA-177934");
    writer.addNodeProperty<types::Int64>(rxnEGFRBinding, "dbId", 177934);

    auto rxnEGFRDimer = writer.addNode({"ReactionLikeEvent"});
    writer.addNodeProperty<types::String>(
        rxnEGFRDimer, "displayName", "EGFR dimerization");
    writer.addNodeProperty<types::String>(rxnEGFRDimer, "stId", "R-HSA-177940");
    writer.addNodeProperty<types::Int64>(rxnEGFRDimer, "dbId", 177940);

    auto rxnEGFREndocytosis = writer.addNode({"ReactionLikeEvent"});
    writer.addNodeProperty<types::String>(
        rxnEGFREndocytosis, "displayName", "EGFR internalization");
    writer.addNodeProperty<types::String>(rxnEGFREndocytosis, "stId", "R-HSA-182969");
    writer.addNodeProperty<types::Int64>(rxnEGFREndocytosis, "dbId", 182969);

    auto rxnAPOELipid = writer.addNode({"ReactionLikeEvent"});
    writer.addNodeProperty<types::String>(
        rxnAPOELipid, "displayName", "APOE4 binds lipid particle");
    writer.addNodeProperty<types::String>(rxnAPOELipid, "stId", "R-HSA-8866913");
    writer.addNodeProperty<types::Int64>(rxnAPOELipid, "dbId", 8866913);

    auto rxnLipidClearance = writer.addNode({"ReactionLikeEvent"});
    writer.addNodeProperty<types::String>(
        rxnLipidClearance, "displayName", "Lipid particle clearance");
    writer.addNodeProperty<types::String>(rxnLipidClearance, "stId", "R-HSA-8866430");
    writer.addNodeProperty<types::Int64>(rxnLipidClearance, "dbId", 8866430);

    auto rxnULKActivation = writer.addNode({"ReactionLikeEvent"});
    writer.addNodeProperty<types::String>(
        rxnULKActivation, "displayName", "ULK1 complex activation");
    writer.addNodeProperty<types::String>(rxnULKActivation, "stId", "R-HSA-5679091");
    writer.addNodeProperty<types::Int64>(rxnULKActivation, "dbId", 5679091);

    auto rxnPhagophore = writer.addNode({"ReactionLikeEvent"});
    writer.addNodeProperty<types::String>(
        rxnPhagophore, "displayName", "Phagophore nucleation");
    writer.addNodeProperty<types::String>(rxnPhagophore, "stId", "R-HSA-5679095");
    writer.addNodeProperty<types::Int64>(rxnPhagophore, "dbId", 5679095);

    // =================================================================
    // ReferenceGeneProducts
    // =================================================================
    auto geneEGFR = writer.addNode({"ReferenceGeneProduct"});
    writer.addNodeProperty<types::String>(geneEGFR, "displayName", "EGFR");
    writer.addNodeProperty<types::String>(geneEGFR, "geneName", "EGFR");
    writer.addNodeProperty<types::String>(geneEGFR, "identifier", "P00533");
    writer.addNodeProperty<types::Int64>(geneEGFR, "dbId", 50720);

    auto geneAPOE = writer.addNode({"ReferenceGeneProduct"});
    writer.addNodeProperty<types::String>(geneAPOE, "displayName", "APOE");
    writer.addNodeProperty<types::String>(geneAPOE, "geneName", "APOE");
    writer.addNodeProperty<types::String>(geneAPOE, "identifier", "P02649");
    writer.addNodeProperty<types::Int64>(geneAPOE, "dbId", 50755);

    auto geneULK1 = writer.addNode({"ReferenceGeneProduct"});
    writer.addNodeProperty<types::String>(geneULK1, "displayName", "ULK1");
    writer.addNodeProperty<types::String>(geneULK1, "geneName", "ULK1");
    writer.addNodeProperty<types::String>(geneULK1, "identifier", "O75385");
    writer.addNodeProperty<types::Int64>(geneULK1, "dbId", 51091);

    auto geneBECN1 = writer.addNode({"ReferenceGeneProduct"});
    writer.addNodeProperty<types::String>(geneBECN1, "displayName", "BECN1");
    writer.addNodeProperty<types::String>(geneBECN1, "geneName", "BECN1");
    writer.addNodeProperty<types::String>(geneBECN1, "identifier", "Q14457");
    writer.addNodeProperty<types::Int64>(geneBECN1, "dbId", 51110);

    // Mouse orthologs (for cross-species gene product join)
    auto geneEgfrMouse = writer.addNode({"ReferenceGeneProduct"});
    writer.addNodeProperty<types::String>(geneEgfrMouse, "displayName", "Egfr");
    writer.addNodeProperty<types::String>(geneEgfrMouse, "geneName", "EGFR");
    writer.addNodeProperty<types::String>(geneEgfrMouse, "identifier", "Q01279");
    writer.addNodeProperty<types::Int64>(geneEgfrMouse, "dbId", 60720);

    auto geneApoeMouse = writer.addNode({"ReferenceGeneProduct"});
    writer.addNodeProperty<types::String>(geneApoeMouse, "displayName", "Apoe");
    writer.addNodeProperty<types::String>(geneApoeMouse, "geneName", "APOE");
    writer.addNodeProperty<types::String>(geneApoeMouse, "identifier", "P08226");
    writer.addNodeProperty<types::Int64>(geneApoeMouse, "dbId", 60755);

    // =================================================================
    // PhysicalEntities (proteins in cellular context)
    // =================================================================
    auto peEGFR = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peEGFR, "displayName", "EGFR [plasma membrane]");
    writer.addNodeProperty<types::Int64>(peEGFR, "dbId", 80001);

    auto peEGF = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peEGF, "displayName", "EGF [extracellular region]");
    writer.addNodeProperty<types::Int64>(peEGF, "dbId", 80002);

    auto peAPOE4 = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peAPOE4, "displayName", "APOE-4 [extracellular region]");
    writer.addNodeProperty<types::Int64>(peAPOE4, "dbId", 80003);

    auto peLipidParticle = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peLipidParticle, "displayName", "Lipid particle [extracellular region]");
    writer.addNodeProperty<types::Int64>(peLipidParticle, "dbId", 80004);

    auto peULK1 = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peULK1, "displayName", "ULK1 [cytosol]");
    writer.addNodeProperty<types::Int64>(peULK1, "dbId", 80005);

    auto peBECN1 = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peBECN1, "displayName", "BECN1 [cytosol]");
    writer.addNodeProperty<types::Int64>(peBECN1, "dbId", 80006);

    auto peEGFRDimer = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peEGFRDimer, "displayName", "EGFR dimer [plasma membrane]");
    writer.addNodeProperty<types::Int64>(peEGFRDimer, "dbId", 80007);

    auto peEGFREndosome = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peEGFREndosome, "displayName", "EGFR [endosome membrane]");
    writer.addNodeProperty<types::Int64>(peEGFREndosome, "dbId", 80008);

    auto peAPOE4Lipid = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        peAPOE4Lipid, "displayName", "APOE4:lipid particle [extracellular region]");
    writer.addNodeProperty<types::Int64>(peAPOE4Lipid, "dbId", 80009);

    auto pePhagophore = writer.addNode({"PhysicalEntity"});
    writer.addNodeProperty<types::String>(
        pePhagophore, "displayName", "Phagophore [cytosol]");
    writer.addNodeProperty<types::Int64>(pePhagophore, "dbId", 80010);

    // =================================================================
    // Complexes
    // =================================================================
    auto cxEGFR_EGF = writer.addNode({"Complex"});
    writer.addNodeProperty<types::String>(
        cxEGFR_EGF, "displayName", "EGF:EGFR complex");
    writer.addNodeProperty<types::Int64>(cxEGFR_EGF, "dbId", 90001);

    auto cxULK = writer.addNode({"Complex"});
    writer.addNodeProperty<types::String>(
        cxULK, "displayName", "ULK1:ATG13:FIP200 complex");
    writer.addNodeProperty<types::Int64>(cxULK, "dbId", 90002);

    auto cxBECN1_PI3K = writer.addNode({"Complex"});
    writer.addNodeProperty<types::String>(
        cxBECN1_PI3K, "displayName", "BECN1:PIK3C3 complex");
    writer.addNodeProperty<types::Int64>(cxBECN1_PI3K, "dbId", 90003);

    auto cxAPOE4Lipid = writer.addNode({"Complex"});
    writer.addNodeProperty<types::String>(
        cxAPOE4Lipid, "displayName", "APOE4:Lipid complex");
    writer.addNodeProperty<types::Int64>(cxAPOE4Lipid, "dbId", 90004);

    // =================================================================
    // hasEvent edges: Pathway -> ReactionLikeEvent
    // =================================================================
    // Signal Transduction -> EGFR sub-pathway -> reactions
    writer.addEdge("hasEvent", tlpSignal, pwEGFR);
    writer.addEdge("hasEvent", tlpSignal, pwEGFRDownreg);
    writer.addEdge("hasEvent", pwEGFR, rxnEGFRBinding);
    writer.addEdge("hasEvent", pwEGFR, rxnEGFRDimer);
    writer.addEdge("hasEvent", pwEGFRDownreg, rxnEGFREndocytosis);

    // Metabolism -> APOE pathway -> reactions
    writer.addEdge("hasEvent", tlpMetabolism, pwAPOE);
    writer.addEdge("hasEvent", tlpMetabolism, pwLipidTransport);
    writer.addEdge("hasEvent", pwAPOE, rxnAPOELipid);
    writer.addEdge("hasEvent", pwAPOE, rxnLipidClearance);
    writer.addEdge("hasEvent", pwLipidTransport, rxnLipidClearance);

    // Autophagy -> Macro-autophagy -> reactions
    writer.addEdge("hasEvent", tlpAutophagy, pwMacroAutophagy);
    writer.addEdge("hasEvent", pwMacroAutophagy, rxnULKActivation);
    writer.addEdge("hasEvent", pwMacroAutophagy, rxnPhagophore);

    // =================================================================
    // species edges
    // =================================================================
    writer.addEdge("species", tlpSignal, homoSapiens);
    writer.addEdge("species", tlpMetabolism, homoSapiens);
    writer.addEdge("species", tlpAutophagy, homoSapiens);
    writer.addEdge("species", pwEGFR, homoSapiens);
    writer.addEdge("species", pwAPOE, homoSapiens);
    writer.addEdge("species", pwMacroAutophagy, homoSapiens);
    writer.addEdge("species", pwEGFRDownreg, homoSapiens);
    writer.addEdge("species", pwLipidTransport, homoSapiens);

    writer.addEdge("species", geneEGFR, homoSapiens);
    writer.addEdge("species", geneAPOE, homoSapiens);
    writer.addEdge("species", geneULK1, homoSapiens);
    writer.addEdge("species", geneBECN1, homoSapiens);
    writer.addEdge("species", geneEgfrMouse, musMusculus);
    writer.addEdge("species", geneApoeMouse, musMusculus);

    // =================================================================
    // hasComponent edges: Complex -> PhysicalEntity
    // =================================================================
    writer.addEdge("hasComponent", cxEGFR_EGF, peEGFR);
    writer.addEdge("hasComponent", cxEGFR_EGF, peEGF);
    writer.addEdge("hasComponent", cxULK, peULK1);
    writer.addEdge("hasComponent", cxBECN1_PI3K, peBECN1);
    writer.addEdge("hasComponent", cxAPOE4Lipid, peAPOE4);
    writer.addEdge("hasComponent", cxAPOE4Lipid, peLipidParticle);

    // Shared component: ULK1 appears in both ULK complex and BECN1 complex
    writer.addEdge("hasComponent", cxBECN1_PI3K, peULK1);

    // =================================================================
    // input / output edges: ReactionLikeEvent -> PhysicalEntity
    // =================================================================
    writer.addEdge("input", rxnEGFRBinding, peEGFR);
    writer.addEdge("input", rxnEGFRBinding, peEGF);
    writer.addEdge("output", rxnEGFRBinding, peEGFRDimer);

    writer.addEdge("input", rxnEGFRDimer, peEGFRDimer);
    writer.addEdge("output", rxnEGFRDimer, peEGFRDimer);

    writer.addEdge("input", rxnEGFREndocytosis, peEGFRDimer);
    writer.addEdge("output", rxnEGFREndocytosis, peEGFREndosome);

    writer.addEdge("input", rxnAPOELipid, peAPOE4);
    writer.addEdge("input", rxnAPOELipid, peLipidParticle);
    writer.addEdge("output", rxnAPOELipid, peAPOE4Lipid);

    writer.addEdge("input", rxnLipidClearance, peAPOE4Lipid);
    writer.addEdge("output", rxnLipidClearance, peLipidParticle);

    writer.addEdge("input", rxnULKActivation, peULK1);
    writer.addEdge("output", rxnULKActivation, peULK1);

    writer.addEdge("input", rxnPhagophore, peBECN1);
    writer.addEdge("output", rxnPhagophore, pePhagophore);

    // =================================================================
    // compartment edges: PhysicalEntity -> Compartment
    // =================================================================
    writer.addEdge("compartment", peEGFR, plasmaMembrane);
    writer.addEdge("compartment", peEGF, extracellular);
    writer.addEdge("compartment", peAPOE4, extracellular);
    writer.addEdge("compartment", peLipidParticle, extracellular);
    writer.addEdge("compartment", peULK1, cytosol);
    writer.addEdge("compartment", peBECN1, cytosol);
    writer.addEdge("compartment", peEGFRDimer, plasmaMembrane);
    writer.addEdge("compartment", peEGFREndosome, endosome);
    writer.addEdge("compartment", peAPOE4Lipid, extracellular);
    writer.addEdge("compartment", pePhagophore, cytosol);

    writer.submit();
}
