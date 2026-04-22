#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"
#include "writers/GraphWriter.h"

#include "LineContainer.h"
#include "TuringException.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

// =============================================================================
// Miniature Reactome graph (~80 nodes, ~90 edges)
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

namespace {

class ReactomeGraph {
public:
    static void create(Graph* graph) {
        GraphWriter writer(graph);
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

        // Shared reaction: lipid clearance appears in both APOE and lipid transport
        // pathways (used for the "pathways sharing reaction" VHJ query)

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
};

} // namespace

// =============================================================================
// Test fixture
// =============================================================================

class ReactomeTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        ReactomeGraph::create(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "reactome";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    auto query(std::string_view query, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        auto res = _db->query(query, state);
        if (!res) {
            spdlog::error("Query failed: {}", res.getError());
        }
        return res;
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }

    PropertyTypeID getPropID(std::string_view propertyName) {
        auto propOpt = read().getView().metadata().propTypes().get(propertyName);
        if (!propOpt) {
            throw TuringException(
                fmt::format("Failed to get property: {}.", propertyName));
        }
        return propOpt->_id;
    }

    LabelID getLabelID(std::string_view labelName) {
        auto labelOpt = read().getView().metadata().labels().get(labelName);
        if (!labelOpt) {
            throw TuringException(
                fmt::format("Failed to get label: {}.", labelName));
        }
        return *labelOpt;
    }
};

// Also test with VHJ forced on
class ReactomeVHJTest : public ReactomeTest {
public:
    void initialize() override {
        ReactomeTest::initialize();
        _queryConfig.getPlanGenConfig().setForceValueHashJoin(true);
    }
};

// =============================================================================
// Basic graph structure tests
// =============================================================================

TEST_F(ReactomeTest, graphHasExpectedNodeCount) {
    uint64_t nodeCount = 0;
    auto res = query("MATCH (n) RETURN count(n)", [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->getLogicalRowCount(), 1);
        auto* col = df->cols()[0]->as<ColumnConst<uint64_t>>();
        ASSERT_TRUE(col);
        nodeCount = col->at(0);
    });
    ASSERT_TRUE(res);
    // 2 Species + 4 Compartments + 3 TLP + 5 Pathways + 7 Reactions
    // + 6 GeneProducts + 10 PhysicalEntities + 4 Complexes = 41
    EXPECT_EQ(nodeCount, 41);
}

TEST_F(ReactomeTest, topLevelPathwayCount) {
    uint64_t count = 0;
    auto res = query(
        "MATCH (n:TopLevelPathway) RETURN count(n)",
        [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            auto* col = df->cols()[0]->as<ColumnConst<uint64_t>>();
            ASSERT_TRUE(col);
            count = col->at(0);
        });
    ASSERT_TRUE(res);
    EXPECT_EQ(count, 3);
}

// =============================================================================
// Q1: Pathways sharing a reaction (stId join)
// =============================================================================

TEST_F(ReactomeVHJTest, pathwaysSharingReaction) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Pathway)-[:hasEvent]->(r1:ReactionLikeEvent),
              (b:Pathway)-[:hasEvent]->(r2:ReactionLikeEvent)
        WHERE r1.stId = r2.stId AND a.dbId <> b.dbId
        RETURN a.displayName, b.displayName, r1.stId
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.displayName")->as<ColumnOptVector<String>>();
        auto* bCol = findColumn(df, "b.displayName")->as<ColumnOptVector<String>>();
        auto* rCol = findColumn(df, "r1.stId")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol && rCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            actual.add({aCol->at(i), bCol->at(i), rCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("APOE4-mediated lipid transport"),
                  String("Lipid particle transport"),
                  String("R-HSA-8866430")});
    expected.add({String("Lipid particle transport"),
                  String("APOE4-mediated lipid transport"),
                  String("R-HSA-8866430")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Q2: Complexes sharing a component with the same displayName
// =============================================================================

TEST_F(ReactomeVHJTest, sharedComplexComponents) {
    constexpr std::string_view QUERY = R"(
        MATCH (c1:Complex)-[:hasComponent]->(e1:PhysicalEntity),
              (c2:Complex)-[:hasComponent]->(e2:PhysicalEntity)
        WHERE e1.displayName = e2.displayName AND c1.dbId <> c2.dbId
        RETURN c1.displayName, c2.displayName, e1.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* c1Col = findColumn(df, "c1.displayName")->as<ColumnOptVector<String>>();
        auto* c2Col = findColumn(df, "c2.displayName")->as<ColumnOptVector<String>>();
        auto* eCol = findColumn(df, "e1.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(c1Col && c2Col && eCol);
        for (size_t i = 0; i < c1Col->size(); i++) {
            actual.add({c1Col->at(i), c2Col->at(i), eCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("ULK1:ATG13:FIP200 complex"),
                  String("BECN1:PIK3C3 complex"),
                  String("ULK1 [cytosol]")});
    expected.add({String("BECN1:PIK3C3 complex"),
                  String("ULK1:ATG13:FIP200 complex"),
                  String("ULK1 [cytosol]")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Q3: Pathways joined by same species (integer hash join on dbId)
// =============================================================================

TEST_F(ReactomeVHJTest, sameSpeciesPathwayJoin) {
    // All 8 pathways point to Homo sapiens -> 8*7 = 56 ordered pairs
    constexpr std::string_view QUERY = R"(
        MATCH (p1:Pathway)-[:species]->(s1:Species),
              (p2:Pathway)-[:species]->(s2:Species)
        WHERE s1.dbId = s2.dbId AND p1.dbId <> p2.dbId
        RETURN p1.displayName, p2.displayName, s1.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* p1Col = findColumn(df, "p1.displayName")->as<ColumnOptVector<String>>();
        auto* p2Col = findColumn(df, "p2.displayName")->as<ColumnOptVector<String>>();
        auto* sCol = findColumn(df, "s1.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(p1Col && p2Col && sCol);
        for (size_t i = 0; i < p1Col->size(); i++) {
            actual.add({p1Col->at(i), p2Col->at(i), sCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // All 8 pathways have species Homo sapiens
    const std::vector<String> pathways = {
        "Signal Transduction",
        "Metabolism of lipids",
        "Autophagy",
        "Signaling by EGFR",
        "APOE4-mediated lipid transport",
        "Macro-autophagy initiation",
        "EGFR downregulation",
        "Lipid particle transport",
    };

    Rows expected;
    for (const auto& p1 : pathways) {
        for (const auto& p2 : pathways) {
            if (p1 != p2) {
                expected.add({p1, p2, String("Homo sapiens")});
            }
        }
    }

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Q4: Gene products with same geneName across different species
// =============================================================================

TEST_F(ReactomeVHJTest, crossSpeciesGeneProducts) {
    constexpr std::string_view QUERY = R"(
        MATCH (g1:ReferenceGeneProduct)-[:species]->(s1:Species),
              (g2:ReferenceGeneProduct)-[:species]->(s2:Species)
        WHERE g1.geneName = g2.geneName AND s1.displayName <> s2.displayName
        RETURN g1.identifier, g2.identifier, g1.geneName,
               s1.displayName, s2.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* g1Col = findColumn(df, "g1.identifier")->as<ColumnOptVector<String>>();
        auto* g2Col = findColumn(df, "g2.identifier")->as<ColumnOptVector<String>>();
        auto* gnCol = findColumn(df, "g1.geneName")->as<ColumnOptVector<String>>();
        auto* s1Col = findColumn(df, "s1.displayName")->as<ColumnOptVector<String>>();
        auto* s2Col = findColumn(df, "s2.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(g1Col && g2Col && gnCol && s1Col && s2Col);
        for (size_t i = 0; i < g1Col->size(); i++) {
            actual.add({g1Col->at(i), g2Col->at(i), gnCol->at(i),
                        s1Col->at(i), s2Col->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("P00533"), String("Q01279"), String("EGFR"),
                  String("Homo sapiens"), String("Mus musculus")});
    expected.add({String("Q01279"), String("P00533"), String("EGFR"),
                  String("Mus musculus"), String("Homo sapiens")});
    expected.add({String("P02649"), String("P08226"), String("APOE"),
                  String("Homo sapiens"), String("Mus musculus")});
    expected.add({String("P08226"), String("P02649"), String("APOE"),
                  String("Mus musculus"), String("Homo sapiens")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Q5: Reactions whose inputs/outputs share a compartment
// =============================================================================

TEST_F(ReactomeVHJTest, sameCompartmentReactions) {
    constexpr std::string_view QUERY = R"(
        MATCH (r:ReactionLikeEvent)-[:input]->(p:PhysicalEntity)
                  -[:compartment]->(c1:Compartment),
              (r2:ReactionLikeEvent)-[:output]->(p2:PhysicalEntity)
                  -[:compartment]->(c2:Compartment)
        WHERE c1.displayName = c2.displayName
        RETURN r.displayName, r2.displayName, c1.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* rCol = findColumn(df, "r.displayName")->as<ColumnOptVector<String>>();
        auto* r2Col = findColumn(df, "r2.displayName")->as<ColumnOptVector<String>>();
        auto* cCol = findColumn(df, "c1.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(rCol && r2Col && cCol);
        for (size_t i = 0; i < rCol->size(); i++) {
            actual.add({rCol->at(i), r2Col->at(i), cCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // Input side (reaction, physEntity, compartment):
    //   EGF binds EGFR      -> EGFR [plasma membrane]      -> plasma membrane
    //   EGF binds EGFR      -> EGF [extracellular region]  -> extracellular region
    //   EGFR dimerization    -> EGFR dimer [plasma membrane]-> plasma membrane
    //   EGFR internalization -> EGFR dimer [plasma membrane]-> plasma membrane
    //   APOE4 binds lipid..  -> APOE-4 [extracellular..]   -> extracellular region
    //   APOE4 binds lipid..  -> Lipid particle [extrac..]   -> extracellular region
    //   Lipid particle clear. -> APOE4:lipid [extrac..]     -> extracellular region
    //   ULK1 complex activ.  -> ULK1 [cytosol]             -> cytosol
    //   Phagophore nucleation-> BECN1 [cytosol]             -> cytosol
    //
    // Output side (reaction, physEntity, compartment):
    //   EGF binds EGFR      -> EGFR dimer [plasma membrane]-> plasma membrane
    //   EGFR dimerization    -> EGFR dimer [plasma membrane]-> plasma membrane
    //   EGFR internalization -> EGFR [endosome membrane]    -> endosome membrane
    //   APOE4 binds lipid..  -> APOE4:lipid [extrac..]     -> extracellular region
    //   Lipid particle clear. -> Lipid particle [extrac..]  -> extracellular region
    //   ULK1 complex activ.  -> ULK1 [cytosol]             -> cytosol
    //   Phagophore nucleation-> Phagophore [cytosol]        -> cytosol

    const String bind = "EGF binds EGFR";
    const String dimer = "EGFR dimerization";
    const String intern = "EGFR internalization";
    const String apoe = "APOE4 binds lipid particle";
    const String lipClr = "Lipid particle clearance";
    const String ulk = "ULK1 complex activation";
    const String phago = "Phagophore nucleation";
    const String pm = "plasma membrane";
    const String ec = "extracellular region";
    const String cy = "cytosol";

    Rows expected;
    // plasma membrane: 3 inputs x 2 outputs = 6
    expected.add({bind,   bind,  pm});
    expected.add({bind,   dimer, pm});
    expected.add({dimer,  bind,  pm});
    expected.add({dimer,  dimer, pm});
    expected.add({intern, bind,  pm});
    expected.add({intern, dimer, pm});

    // extracellular region: 4 inputs x 2 outputs = 8
    expected.add({bind,   apoe,   ec});
    expected.add({bind,   lipClr, ec});
    expected.add({apoe,   apoe,   ec});  // from peAPOE4
    expected.add({apoe,   apoe,   ec});  // from peLipidParticle
    expected.add({apoe,   lipClr, ec});  // from peAPOE4
    expected.add({apoe,   lipClr, ec});  // from peLipidParticle
    expected.add({lipClr, apoe,   ec});
    expected.add({lipClr, lipClr, ec});

    // cytosol: 2 inputs x 2 outputs = 4
    expected.add({ulk,   ulk,   cy});
    expected.add({ulk,   phago, cy});
    expected.add({phago, ulk,   cy});
    expected.add({phago, phago, cy});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Q6: Edge ID join – same edge variable across two patterns
// =============================================================================

TEST_F(ReactomeVHJTest, edgeIdJoinHasEvent) {
    // Each hasEvent edge self-joins via the shared edge variable e
    constexpr std::string_view QUERY = R"(
        MATCH (a:Pathway)-[e:hasEvent]->(b), (c)-[e]->(d)
        RETURN a.displayName, b.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.displayName")->as<ColumnOptVector<String>>();
        auto* bCol = findColumn(df, "b.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            actual.add({aCol->at(i), bCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // 13 hasEvent edges in the graph
    Rows expected;
    // Signal Transduction -> sub-pathways
    expected.add({String("Signal Transduction"),
                  String("Signaling by EGFR")});
    expected.add({String("Signal Transduction"),
                  String("EGFR downregulation")});
    // Signaling by EGFR -> reactions
    expected.add({String("Signaling by EGFR"),
                  String("EGF binds EGFR")});
    expected.add({String("Signaling by EGFR"),
                  String("EGFR dimerization")});
    // EGFR downregulation -> reaction
    expected.add({String("EGFR downregulation"),
                  String("EGFR internalization")});
    // Metabolism of lipids -> sub-pathways
    expected.add({String("Metabolism of lipids"),
                  String("APOE4-mediated lipid transport")});
    expected.add({String("Metabolism of lipids"),
                  String("Lipid particle transport")});
    // APOE4-mediated lipid transport -> reactions
    expected.add({String("APOE4-mediated lipid transport"),
                  String("APOE4 binds lipid particle")});
    expected.add({String("APOE4-mediated lipid transport"),
                  String("Lipid particle clearance")});
    // Lipid particle transport -> reaction (shared)
    expected.add({String("Lipid particle transport"),
                  String("Lipid particle clearance")});
    // Autophagy -> sub-pathway
    expected.add({String("Autophagy"),
                  String("Macro-autophagy initiation")});
    // Macro-autophagy initiation -> reactions
    expected.add({String("Macro-autophagy initiation"),
                  String("ULK1 complex activation")});
    expected.add({String("Macro-autophagy initiation"),
                  String("Phagophore nucleation")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Shared-source fan-out with cross-pattern predicate (issue #397 motif)
//   MATCH (x)-->(a), (x)-->(b) WHERE predicate(a, b) RETURN ...
// =============================================================================

// TODO: Enable when issue #397 (incorrect dependency detection) is fixed.
TEST_F(ReactomeVHJTest, DISABLED_sharedSourcePathwayFanOut) {
    // Pathway x fans out to two distinct events a and b
    constexpr std::string_view QUERY = R"(
        MATCH (x:Pathway)-[:hasEvent]->(a),
              (x)-[:hasEvent]->(b)
        WHERE a.dbId <> b.dbId
        RETURN x.displayName, a.displayName, b.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* xCol = findColumn(df, "x.displayName")->as<ColumnOptVector<String>>();
        auto* aCol = findColumn(df, "a.displayName")->as<ColumnOptVector<String>>();
        auto* bCol = findColumn(df, "b.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(xCol && aCol && bCol);
        for (size_t i = 0; i < xCol->size(); i++) {
            actual.add({xCol->at(i), aCol->at(i), bCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // Pathways with 2+ hasEvent edges produce ordered pairs:
    //   Signal Transduction:           pwEGFR, pwEGFRDownreg
    //   Metabolism of lipids:          pwAPOE, pwLipidTransport
    //   Signaling by EGFR:            rxnEGFRBinding, rxnEGFRDimer
    //   APOE4-mediated lipid transp.: rxnAPOELipid, rxnLipidClearance
    //   Macro-autophagy initiation:   rxnULKActivation, rxnPhagophore
    Rows expected;
    expected.add({String("Signal Transduction"),
                  String("Signaling by EGFR"),
                  String("EGFR downregulation")});
    expected.add({String("Signal Transduction"),
                  String("EGFR downregulation"),
                  String("Signaling by EGFR")});
    expected.add({String("Metabolism of lipids"),
                  String("APOE4-mediated lipid transport"),
                  String("Lipid particle transport")});
    expected.add({String("Metabolism of lipids"),
                  String("Lipid particle transport"),
                  String("APOE4-mediated lipid transport")});
    expected.add({String("Signaling by EGFR"),
                  String("EGF binds EGFR"),
                  String("EGFR dimerization")});
    expected.add({String("Signaling by EGFR"),
                  String("EGFR dimerization"),
                  String("EGF binds EGFR")});
    expected.add({String("APOE4-mediated lipid transport"),
                  String("APOE4 binds lipid particle"),
                  String("Lipid particle clearance")});
    expected.add({String("APOE4-mediated lipid transport"),
                  String("Lipid particle clearance"),
                  String("APOE4 binds lipid particle")});
    expected.add({String("Macro-autophagy initiation"),
                  String("ULK1 complex activation"),
                  String("Phagophore nucleation")});
    expected.add({String("Macro-autophagy initiation"),
                  String("Phagophore nucleation"),
                  String("ULK1 complex activation")});

    EXPECT_TRUE(expected.equals(actual));
}

// TODO: Enable when issue #397 (incorrect dependency detection) is fixed.
TEST_F(ReactomeVHJTest, DISABLED_sharedSourceReactionInputPairs) {
    // Reaction x fans out via input to two distinct physical entities
    constexpr std::string_view QUERY = R"(
        MATCH (x:ReactionLikeEvent)-[:input]->(a:PhysicalEntity),
              (x)-[:input]->(b:PhysicalEntity)
        WHERE a.dbId <> b.dbId
        RETURN x.displayName, a.displayName, b.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* xCol = findColumn(df, "x.displayName")->as<ColumnOptVector<String>>();
        auto* aCol = findColumn(df, "a.displayName")->as<ColumnOptVector<String>>();
        auto* bCol = findColumn(df, "b.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(xCol && aCol && bCol);
        for (size_t i = 0; i < xCol->size(); i++) {
            actual.add({xCol->at(i), aCol->at(i), bCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // Reactions with 2+ input edges:
    //   EGF binds EGFR:          peEGFR, peEGF
    //   APOE4 binds lipid part.: peAPOE4, peLipidParticle
    Rows expected;
    expected.add({String("EGF binds EGFR"),
                  String("EGFR [plasma membrane]"),
                  String("EGF [extracellular region]")});
    expected.add({String("EGF binds EGFR"),
                  String("EGF [extracellular region]"),
                  String("EGFR [plasma membrane]")});
    expected.add({String("APOE4 binds lipid particle"),
                  String("APOE-4 [extracellular region]"),
                  String("Lipid particle [extracellular region]")});
    expected.add({String("APOE4 binds lipid particle"),
                  String("Lipid particle [extracellular region]"),
                  String("APOE-4 [extracellular region]")});

    EXPECT_TRUE(expected.equals(actual));
}

// TODO: Enable when issue #397 (incorrect dependency detection) is fixed.
TEST_F(ReactomeVHJTest, DISABLED_sharedSourceComponentsSameCompartment) {
    // Complex x fans out to components a and b that share a compartment
    // This is the full issue #397 motif: (x)-->(a), (x)-->(b) with
    // equality predicate on properties reachable from a and b.
    constexpr std::string_view QUERY = R"(
        MATCH (x:Complex)-[:hasComponent]->(a:PhysicalEntity)
                  -[:compartment]->(c1:Compartment),
              (x)-[:hasComponent]->(b:PhysicalEntity)
                  -[:compartment]->(c2:Compartment)
        WHERE c1.displayName = c2.displayName AND a.dbId <> b.dbId
        RETURN x.displayName, a.displayName, b.displayName,
               c1.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* xCol = findColumn(df, "x.displayName")->as<ColumnOptVector<String>>();
        auto* aCol = findColumn(df, "a.displayName")->as<ColumnOptVector<String>>();
        auto* bCol = findColumn(df, "b.displayName")->as<ColumnOptVector<String>>();
        auto* cCol = findColumn(df, "c1.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(xCol && aCol && bCol && cCol);
        for (size_t i = 0; i < xCol->size(); i++) {
            actual.add({xCol->at(i), aCol->at(i), bCol->at(i), cCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // BECN1:PIK3C3 complex: peBECN1 (cytosol), peULK1 (cytosol) -> match
    // APOE4:Lipid complex:  peAPOE4 (extracellular), peLipid (extracellular) -> match
    // EGF:EGFR complex:     peEGFR (plasma membrane), peEGF (extracellular) -> no match
    Rows expected;
    expected.add({String("BECN1:PIK3C3 complex"),
                  String("BECN1 [cytosol]"),
                  String("ULK1 [cytosol]"),
                  String("cytosol")});
    expected.add({String("BECN1:PIK3C3 complex"),
                  String("ULK1 [cytosol]"),
                  String("BECN1 [cytosol]"),
                  String("cytosol")});
    expected.add({String("APOE4:Lipid complex"),
                  String("APOE-4 [extracellular region]"),
                  String("Lipid particle [extracellular region]"),
                  String("extracellular region")});
    expected.add({String("APOE4:Lipid complex"),
                  String("Lipid particle [extracellular region]"),
                  String("APOE-4 [extracellular region]"),
                  String("extracellular region")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Domain-specific queries: EGFR signalling
// =============================================================================

TEST_F(ReactomeTest, egfrSignallingPathway) {
    constexpr std::string_view QUERY = R"(
        MATCH (p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent)
        WHERE p.displayName = 'Signaling by EGFR'
        RETURN r.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* col = findColumn(df, "r.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(col);
        for (size_t i = 0; i < col->size(); i++) {
            actual.add({col->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("EGF binds EGFR")});
    expected.add({String("EGFR dimerization")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Domain-specific queries: APOE-4 lipid metabolism
// =============================================================================

TEST_F(ReactomeTest, apoe4ReactionInputsOutputs) {
    constexpr std::string_view QUERY = R"(
        MATCH (r:ReactionLikeEvent)-[:input]->(i:PhysicalEntity),
              (r)-[:output]->(o:PhysicalEntity)
        WHERE r.displayName = 'APOE4 binds lipid particle'
        RETURN i.displayName, o.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* iCol = findColumn(df, "i.displayName")->as<ColumnOptVector<String>>();
        auto* oCol = findColumn(df, "o.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(iCol && oCol);
        for (size_t i = 0; i < iCol->size(); i++) {
            actual.add({iCol->at(i), oCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("APOE-4 [extracellular region]"),
                  String("APOE4:lipid particle [extracellular region]")});
    expected.add({String("Lipid particle [extracellular region]"),
                  String("APOE4:lipid particle [extracellular region]")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Domain-specific queries: Autophagy
// =============================================================================

TEST_F(ReactomeTest, autophagyReactions) {
    constexpr std::string_view QUERY = R"(
        MATCH (tlp:TopLevelPathway)-[:hasEvent]->(p:Pathway)
                  -[:hasEvent]->(r:ReactionLikeEvent)
        WHERE tlp.displayName = 'Autophagy'
        RETURN p.displayName, r.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* pCol = findColumn(df, "p.displayName")->as<ColumnOptVector<String>>();
        auto* rCol = findColumn(df, "r.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(pCol && rCol);
        for (size_t i = 0; i < pCol->size(); i++) {
            actual.add({pCol->at(i), rCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("Macro-autophagy initiation"),
                  String("ULK1 complex activation")});
    expected.add({String("Macro-autophagy initiation"),
                  String("Phagophore nucleation")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Multi-hop: TopLevelPathway -> Pathway -> Reaction -> PhysicalEntity
// =============================================================================

TEST_F(ReactomeTest, topLevelToPhysicalEntity) {
    constexpr std::string_view QUERY = R"(
        MATCH (tlp:TopLevelPathway)-[:hasEvent]->(p:Pathway)
                  -[:hasEvent]->(r:ReactionLikeEvent)-[:input]->(pe:PhysicalEntity)
        WHERE tlp.displayName = 'Autophagy'
        RETURN r.displayName, pe.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* rCol = findColumn(df, "r.displayName")->as<ColumnOptVector<String>>();
        auto* peCol = findColumn(df, "pe.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(rCol && peCol);
        for (size_t i = 0; i < rCol->size(); i++) {
            actual.add({rCol->at(i), peCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("ULK1 complex activation"), String("ULK1 [cytosol]")});
    expected.add({String("Phagophore nucleation"), String("BECN1 [cytosol]")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Complex component + compartment join
// =============================================================================

TEST_F(ReactomeTest, complexComponentsInCytosol) {
    constexpr std::string_view QUERY = R"(
        MATCH (cx:Complex)-[:hasComponent]->(pe:PhysicalEntity)
                  -[:compartment]->(c:Compartment)
        WHERE c.displayName = 'cytosol'
        RETURN cx.displayName, pe.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* cxCol = findColumn(df, "cx.displayName")->as<ColumnOptVector<String>>();
        auto* peCol = findColumn(df, "pe.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(cxCol && peCol);
        for (size_t i = 0; i < cxCol->size(); i++) {
            actual.add({cxCol->at(i), peCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("ULK1:ATG13:FIP200 complex"), String("ULK1 [cytosol]")});
    expected.add({String("BECN1:PIK3C3 complex"), String("BECN1 [cytosol]")});
    expected.add({String("BECN1:PIK3C3 complex"), String("ULK1 [cytosol]")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Gene product to species lookup
// =============================================================================

TEST_F(ReactomeTest, humanGeneProducts) {
    constexpr std::string_view QUERY = R"(
        MATCH (g:ReferenceGeneProduct)-[:species]->(s:Species)
        WHERE s.displayName = 'Homo sapiens'
        RETURN g.geneName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString>;

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* col = findColumn(df, "g.geneName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(col);
        for (size_t i = 0; i < col->size(); i++) {
            actual.add({col->at(i)});
        }
    });
    ASSERT_TRUE(res);

    Rows expected;
    expected.add({String("EGFR")});
    expected.add({String("APOE")});
    expected.add({String("ULK1")});
    expected.add({String("BECN1")});

    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// Verify VHJ and non-VHJ produce same results for compartment query
// =============================================================================

TEST_F(ReactomeVHJTest, sameCompartmentReactionsMatchesNonVHJ) {
    constexpr std::string_view QUERY = R"(
        MATCH (r:ReactionLikeEvent)-[:input]->(p:PhysicalEntity)
                  -[:compartment]->(c1:Compartment),
              (r2:ReactionLikeEvent)-[:output]->(p2:PhysicalEntity)
                  -[:compartment]->(c2:Compartment)
        WHERE c1.displayName = c2.displayName
        RETURN r.displayName, r2.displayName, c1.displayName
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    Rows vhjActual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* rCol = findColumn(df, "r.displayName")->as<ColumnOptVector<String>>();
        auto* r2Col = findColumn(df, "r2.displayName")->as<ColumnOptVector<String>>();
        auto* cCol = findColumn(df, "c1.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(rCol && r2Col && cCol);
        for (size_t i = 0; i < rCol->size(); i++) {
            vhjActual.add({rCol->at(i), r2Col->at(i), cCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // Run again without VHJ
    QueryConfig noVhjConfig;
    noVhjConfig.getPlanGenConfig().setUseValueHashJoin(false);
    Rows nonVhjActual;
    QueryCallbacks noVhjCallbacks;
    noVhjCallbacks.setOnOutputData([&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* rCol = findColumn(df, "r.displayName")->as<ColumnOptVector<String>>();
        auto* r2Col = findColumn(df, "r2.displayName")->as<ColumnOptVector<String>>();
        auto* cCol = findColumn(df, "c1.displayName")->as<ColumnOptVector<String>>();
        ASSERT_TRUE(rCol && r2Col && cCol);
        for (size_t i = 0; i < rCol->size(); i++) {
            nonVhjActual.add({rCol->at(i), r2Col->at(i), cCol->at(i)});
        }
    });
    const QueryState noVhjState(_graphName, &_env->getMem(), &noVhjConfig, &noVhjCallbacks);
    auto res2 = _db->query(QUERY, noVhjState);
    ASSERT_TRUE(res2);

    EXPECT_TRUE(vhjActual.equals(nonVhjActual));
}
