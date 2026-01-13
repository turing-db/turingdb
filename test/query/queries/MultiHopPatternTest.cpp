#include <gtest/gtest.h>

#include <string_view>
#include <set>
#include <map>
#include <vector>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringException.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

// =============================================================================
// MULTI-HOP PATTERN TESTS
// Tests for complex Cypher queries with multiple edge patterns like:
// (a)-->(b)<--(c), (a)<--(b)-->(c), (a)-->(b)-->(c), etc.
// =============================================================================

class MultiHopPatternTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), ChangeID::head());
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

// =============================================================================
// CATEGORY 1: DIVERGENT PATTERNS (a)-->(b)<--(c)
// These directly test the bug scenario where middle node should be Interest
// =============================================================================

// THE PRIMARY BUG-CATCHING TEST
// Query finds shared interests between French and non-French persons
// Expected: "Cooking" (Adam[French] --> Cooking <-- Martina[non-French])
// Bug: Returns "Remy" (Person node instead of Interest)
TEST_F(MultiHopPatternTest, divergentPattern_sharedInterestFrenchNonFrench) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (pf:Person)-->(i:Interest)<--(npf:Person)
        WHERE pf.isFrench AND NOT npf.isFrench
        RETURN i.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build expected results by manually traversing the graph
    // Find all Interest nodes that have incoming edges from both a French and non-French Person
    std::set<String> expectedInterests;
    {
        // Build map: Interest node -> list of Person nodes interested in it
        std::map<NodeID, std::vector<NodeID>> interestToPersons;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            // e._nodeID is source (Person), e._otherID is target (Interest)
            NodeView srcView = read().getNodeView(e._nodeID);
            NodeView dstView = read().getNodeView(e._otherID);
            if (srcView.labelset().hasLabel(personLabelID) && dstView.labelset().hasLabel(interestLabelID)) {
                interestToPersons[e._otherID].push_back(e._nodeID);
            }
        }

        // For each Interest, check if it has both French and non-French persons
        for (const auto& [interestNode, persons] : interestToPersons) {
            bool hasFrench = false;
            bool hasNonFrench = false;

            for (const NodeID personNode : persons) {
                const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, personNode);
                if (isFrench && *isFrench) {
                    hasFrench = true;
                } else if (isFrench && !*isFrench) {
                    hasNonFrench = true;
                }
            }

            if (hasFrench && hasNonFrench) {
                const auto* name = read().tryGetNodeProperty<types::String>(nameID, interestNode);
                if (name) {
                    expectedInterests.insert(*name);
                }
            }
        }
    }

    Rows expected;
    for (const auto& interest : expectedInterests) {
        expected.add({interest});
    }
    // We expect "Cooking" (Adam[French] and Martina[non-French])
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);
            std::set<String> seen;
            for (size_t row = 0; row < names->size(); row++) {
                if (names->at(row) && seen.find(*names->at(row)) == seen.end()) {
                    actual.add({*names->at(row)});
                    seen.insert(*names->at(row));
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test returning all shared Interest nodes between any two Persons
// DISABLED: DISTINCT not yet supported
TEST_F(MultiHopPatternTest, DISABLED_divergentPattern_returnMiddleNode) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person) RETURN DISTINCT b.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Find all Interest nodes that have at least 2 incoming edges from Person nodes
    std::set<String> expectedInterests;
    {
        std::map<NodeID, int> interestPersonCount;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestPersonCount[e._otherID]++;
            }
        }

        for (const auto& [interestNode, count] : interestPersonCount) {
            if (count >= 2) {
                const auto* name = read().tryGetNodeProperty<types::String>(nameID, interestNode);
                if (name) {
                    expectedInterests.insert(*name);
                }
            }
        }
    }

    Rows expected;
    for (const auto& interest : expectedInterests) {
        expected.add({interest});
    }
    // Should include: Cooking (Adam, Martina), Bio (Adam, Maxime), Computers (Remy, Luc), Gym (Suhas, Cyrus, Doruk)
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);
            std::set<String> seen;
            for (size_t row = 0; row < names->size(); row++) {
                if (names->at(row) && seen.find(*names->at(row)) == seen.end()) {
                    actual.add({*names->at(row)});
                    seen.insert(*names->at(row));
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test returning all three nodes in the pattern
// DISABLED: Comparison of NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_divergentPattern_returnAllThreeNodes) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE a <> c
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    {
        // Build map: Interest -> list of Persons interested in it
        std::map<NodeID, std::vector<NodeID>> interestToPersons;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestToPersons[e._otherID].push_back(e._nodeID);
            }
        }

        // Generate all pairs (a, c) where a != c for each Interest
        for (const auto& [interestNode, persons] : interestToPersons) {
            const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, interestNode);
            if (!interestName) continue;

            for (size_t i = 0; i < persons.size(); i++) {
                for (size_t j = 0; j < persons.size(); j++) {
                    if (persons[i] != persons[j]) {
                        const auto* nameA = read().tryGetNodeProperty<types::String>(nameID, persons[i]);
                        const auto* nameC = read().tryGetNodeProperty<types::String>(nameID, persons[j]);
                        if (nameA && nameC) {
                            expected.add({*nameA, *interestName, *nameC});
                        }
                    }
                }
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesA = findColumn(df, "a.name")->as<ColumnOptVector<String>>();
            auto* namesB = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            auto* namesC = findColumn(df, "c.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesA && namesB && namesC);
            for (size_t row = 0; row < namesA->size(); row++) {
                if (namesA->at(row) && namesB->at(row) && namesC->at(row)) {
                    actual.add({*namesA->at(row), *namesB->at(row), *namesC->at(row)});
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test with explicit edge type constraints
// DISABLED: Comparisons of NodePatterns not yet supported
TEST_F(MultiHopPatternTest, DISABLED_divergentPattern_withEdgeTypes) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-[e1:INTERESTED_IN]->(b:Interest)<-[e2:INTERESTED_IN]-(c:Person) WHERE a <> c RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const EdgeTypeID INTERESTED_IN_TYPEID = 1;

    Rows expected;
    {
        std::map<NodeID, std::vector<NodeID>> interestToPersons;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e._edgeID) != INTERESTED_IN_TYPEID) continue;
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestToPersons[e._otherID].push_back(e._nodeID);
            }
        }

        for (const auto& [interestNode, persons] : interestToPersons) {
            const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, interestNode);
            if (!interestName) continue;

            for (size_t i = 0; i < persons.size(); i++) {
                for (size_t j = 0; j < persons.size(); j++) {
                    if (persons[i] != persons[j]) {
                        const auto* nameA = read().tryGetNodeProperty<types::String>(nameID, persons[i]);
                        const auto* nameC = read().tryGetNodeProperty<types::String>(nameID, persons[j]);
                        if (nameA && nameC) {
                            expected.add({*nameA, *interestName, *nameC});
                        }
                    }
                }
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesA = findColumn(df, "a.name")->as<ColumnOptVector<String>>();
            auto* namesB = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            auto* namesC = findColumn(df, "c.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesA && namesB && namesC);
            for (size_t row = 0; row < namesA->size(); row++) {
                if (namesA->at(row) && namesB->at(row) && namesC->at(row)) {
                    actual.add({*namesA->at(row), *namesB->at(row), *namesC->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test with filter on the middle node property
// DISABLED: Comparison on NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_divergentPattern_filterOnMiddleNode) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.isReal AND a <> c
        RETURN DISTINCT b.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isRealID = getPropID("isReal");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    std::set<String> expectedInterests;
    {
        std::map<NodeID, int> interestPersonCount;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestPersonCount[e._otherID]++;
            }
        }

        for (const auto& [interestNode, count] : interestPersonCount) {
            if (count < 2) continue;

            const auto* isReal = read().tryGetNodeProperty<types::Bool>(isRealID, interestNode);
            if (!isReal || !*isReal) continue;

            const auto* name = read().tryGetNodeProperty<types::String>(nameID, interestNode);
            if (name) {
                expectedInterests.insert(*name);
            }
        }
    }

    Rows expected;
    for (const auto& interest : expectedInterests) {
        expected.add({interest});
    }
    // Should include: Computers (isReal=true, shared by Remy, Luc), Gym (isReal=true, shared by Suhas, Cyrus, Doruk)
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);
            std::set<String> seen;
            for (size_t row = 0; row < names->size(); row++) {
                if (names->at(row) && seen.find(*names->at(row)) == seen.end()) {
                    actual.add({*names->at(row)});
                    seen.insert(*names->at(row));
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 2: CONVERGENT PATTERNS (a)<--(b)-->(c)
// Person with multiple outgoing edges to different targets
// =============================================================================

// Test person with multiple interests
// DISABLED: Comparison on NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_convergentPattern_personWithMultipleInterests) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (i1:Interest)<--(p:Person)-->(i2:Interest)
        WHERE i1 <> i2
        RETURN p.name, i1.name, i2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    {
        // Build map: Person -> list of Interests
        std::map<NodeID, std::vector<NodeID>> personToInterests;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                personToInterests[e._nodeID].push_back(e._otherID);
            }
        }

        // All pairs of different interests for each person
        for (const auto& [personNode, interests] : personToInterests) {
            const auto* personName = read().tryGetNodeProperty<types::String>(nameID, personNode);
            if (!personName) continue;

            for (size_t i = 0; i < interests.size(); i++) {
                for (size_t j = 0; j < interests.size(); j++) {
                    if (interests[i] != interests[j]) {
                        const auto* name1 = read().tryGetNodeProperty<types::String>(nameID, interests[i]);
                        const auto* name2 = read().tryGetNodeProperty<types::String>(nameID, interests[j]);
                        if (name1 && name2) {
                            expected.add({*personName, *name1, *name2});
                        }
                    }
                }
            }
        }
    }
    // Remy has 3 interests, Adam has 2, Maxime has 2, Luc has 2, etc.
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* pNames = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            auto* i1Names = findColumn(df, "i1.name")->as<ColumnOptVector<String>>();
            auto* i2Names = findColumn(df, "i2.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(pNames && i1Names && i2Names);
            for (size_t row = 0; row < pNames->size(); row++) {
                if (pNames->at(row) && i1Names->at(row) && i2Names->at(row)) {
                    actual.add({*pNames->at(row), *i1Names->at(row), *i2Names->at(row)});
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test convergent pattern filtered by French persons
// DISABLED: Comparison on NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_convergentPattern_withFilter) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (i1:Interest)<--(p:Person)-->(i2:Interest)
        WHERE p.isFrench AND i1 <> i2
        RETURN DISTINCT p.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    std::set<String> expectedPersons;
    {
        std::map<NodeID, int> personInterestCount;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                personInterestCount[e._nodeID]++;
            }
        }

        for (const auto& [personNode, count] : personInterestCount) {
            if (count < 2) continue;

            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, personNode);
            if (!isFrench || !*isFrench) continue;

            const auto* name = read().tryGetNodeProperty<types::String>(nameID, personNode);
            if (name) {
                expectedPersons.insert(*name);
            }
        }
    }

    Rows expected;
    for (const auto& person : expectedPersons) {
        expected.add({person});
    }
    // Should include: Remy (3 interests), Adam (2), Maxime (2), Luc (2)
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);
            std::set<String> seen;
            for (size_t row = 0; row < names->size(); row++) {
                if (names->at(row) && seen.find(*names->at(row)) == seen.end()) {
                    actual.add({*names->at(row)});
                    seen.insert(*names->at(row));
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test convergent pattern with mixed edge types
TEST_F(MultiHopPatternTest, convergentPattern_mixedEdgeTypes) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (i:Interest)<-[e1:INTERESTED_IN]-(p:Person)-[e2:KNOWS_WELL]->(f:Person)
        RETURN p.name, i.name, f.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const EdgeTypeID INTERESTED_IN_TYPEID = 1;
    const EdgeTypeID KNOWS_WELL_TYPEID = 0;

    Rows expected;
    {
        // For each person, find their interests and who they know well
        std::map<NodeID, std::vector<NodeID>> personToInterests;
        std::map<NodeID, std::vector<NodeID>> personToKnown;

        for (const EdgeRecord& e : read().scanOutEdges()) {
            const EdgeTypeID edgeType = read().getEdgeTypeID(e._edgeID);
            NodeView srcView = read().getNodeView(e._nodeID);
            NodeView dstView = read().getNodeView(e._otherID);
            if (edgeType == INTERESTED_IN_TYPEID &&
                srcView.labelset().hasLabel(personLabelID) && dstView.labelset().hasLabel(interestLabelID)) {
                personToInterests[e._nodeID].push_back(e._otherID);
            } else if (edgeType == KNOWS_WELL_TYPEID &&
                       srcView.labelset().hasLabel(personLabelID) && dstView.labelset().hasLabel(personLabelID)) {
                personToKnown[e._nodeID].push_back(e._otherID);
            }
        }

        for (const auto& [personNode, interests] : personToInterests) {
            auto knownIt = personToKnown.find(personNode);
            if (knownIt == personToKnown.end()) continue;

            const auto* personName = read().tryGetNodeProperty<types::String>(nameID, personNode);
            if (!personName) continue;

            for (const NodeID interest : interests) {
                for (const NodeID known : knownIt->second) {
                    const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, interest);
                    const auto* knownName = read().tryGetNodeProperty<types::String>(nameID, known);
                    if (interestName && knownName) {
                        expected.add({*personName, *interestName, *knownName});
                    }
                }
            }
        }
    }
    // Remy knows Adam and has interests, Adam knows Remy and has interests
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* pNames = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            auto* iNames = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            auto* fNames = findColumn(df, "f.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(pNames && iNames && fNames);
            for (size_t row = 0; row < pNames->size(); row++) {
                if (pNames->at(row) && iNames->at(row) && fNames->at(row)) {
                    actual.add({*pNames->at(row), *iNames->at(row), *fNames->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 3: FORWARD CHAIN PATTERNS (a)-->(b)-->(c)
// =============================================================================

// Test Person -> Person -> Interest (via KNOWS_WELL -> INTERESTED_IN)
TEST_F(MultiHopPatternTest, forwardChain_personKnowsPersonInterest) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-[:KNOWS_WELL]->(b:Person)-[:INTERESTED_IN]->(c:Interest)
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const EdgeTypeID INTERESTED_IN_TYPEID = 1;
    const EdgeTypeID KNOWS_WELL_TYPEID = 0;

    Rows expected;
    {
        for (const EdgeRecord& e1 : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e1._edgeID) != KNOWS_WELL_TYPEID) continue;
            NodeView srcView1 = read().getNodeView(e1._nodeID);
            NodeView dstView1 = read().getNodeView(e1._otherID);
            if (!srcView1.labelset().hasLabel(personLabelID) || !dstView1.labelset().hasLabel(personLabelID)) continue;

            const NodeID personA = e1._nodeID;
            const NodeID personB = e1._otherID;

            ColumnNodeIDs bNodes;
            bNodes.push_back(personB);

            for (const EdgeRecord& e2 : read().getOutEdges(&bNodes)) {
                if (read().getEdgeTypeID(e2._edgeID) != INTERESTED_IN_TYPEID) continue;
                if (!read().getNodeView(e2._otherID).labelset().hasLabel(interestLabelID)) continue;

                const NodeID interestC = e2._otherID;

                const auto* nameA = read().tryGetNodeProperty<types::String>(nameID, personA);
                const auto* nameB = read().tryGetNodeProperty<types::String>(nameID, personB);
                const auto* nameC = read().tryGetNodeProperty<types::String>(nameID, interestC);
                if (nameA && nameB && nameC) {
                    expected.add({*nameA, *nameB, *nameC});
                }
            }
        }
    }
    // Remy -> Adam -> (Bio, Cooking), Adam -> Remy -> (Ghosts, Computers, Eighties)
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesA = findColumn(df, "a.name")->as<ColumnOptVector<String>>();
            auto* namesB = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            auto* namesC = findColumn(df, "c.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesA && namesB && namesC);
            for (size_t row = 0; row < namesA->size(); row++) {
                if (namesA->at(row) && namesB->at(row) && namesC->at(row)) {
                    actual.add({*namesA->at(row), *namesB->at(row), *namesC->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test forward chain with filters
TEST_F(MultiHopPatternTest, forwardChain_withFilters) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-[:KNOWS_WELL]->(b:Person)-[:INTERESTED_IN]->(c:Interest)
        WHERE a.isFrench
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const EdgeTypeID INTERESTED_IN_TYPEID = 1;
    const EdgeTypeID KNOWS_WELL_TYPEID = 0;

    Rows expected;
    {
        for (const EdgeRecord& e1 : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e1._edgeID) != KNOWS_WELL_TYPEID) continue;
            NodeView srcView1 = read().getNodeView(e1._nodeID);
            NodeView dstView1 = read().getNodeView(e1._otherID);
            if (!srcView1.labelset().hasLabel(personLabelID) || !dstView1.labelset().hasLabel(personLabelID)) continue;

            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, e1._nodeID);
            if (!isFrench || !*isFrench) continue;

            const NodeID personA = e1._nodeID;
            const NodeID personB = e1._otherID;

            ColumnNodeIDs bNodes;
            bNodes.push_back(personB);

            for (const EdgeRecord& e2 : read().getOutEdges(&bNodes)) {
                if (read().getEdgeTypeID(e2._edgeID) != INTERESTED_IN_TYPEID) continue;
                if (!read().getNodeView(e2._otherID).labelset().hasLabel(interestLabelID)) continue;

                const auto* nameA = read().tryGetNodeProperty<types::String>(nameID, personA);
                const auto* nameB = read().tryGetNodeProperty<types::String>(nameID, personB);
                const auto* nameC = read().tryGetNodeProperty<types::String>(nameID, e2._otherID);
                if (nameA && nameB && nameC) {
                    expected.add({*nameA, *nameB, *nameC});
                }
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesA = findColumn(df, "a.name")->as<ColumnOptVector<String>>();
            auto* namesB = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            auto* namesC = findColumn(df, "c.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesA && namesB && namesC);
            for (size_t row = 0; row < namesA->size(); row++) {
                if (namesA->at(row) && namesB->at(row) && namesC->at(row)) {
                    actual.add({*namesA->at(row), *namesB->at(row), *namesC->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test three hops with dead end (Interest nodes have no outgoing edges)
TEST_F(MultiHopPatternTest, forwardChain_deadEnd) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest)-->(x) RETURN p.name, i.name, x.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    Rows expected;
    {
        const PropertyTypeID nameID = getPropID("name");
        const LabelID personLabelID = getLabelID("Person");
        const LabelID interestLabelID = getLabelID("Interest");
        const EdgeTypeID INTERESTED_IN_TYPEID = 1;

        for (const EdgeRecord& e1 : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e1._edgeID) != INTERESTED_IN_TYPEID) continue;
            NodeView srcView1 = read().getNodeView(e1._nodeID);
            NodeView dstView1 = read().getNodeView(e1._otherID);
            if (!srcView1.labelset().hasLabel(personLabelID)
                || !dstView1.labelset().hasLabel(interestLabelID)) continue;

            const NodeID personP = e1._nodeID;
            const NodeID interestI = e1._otherID;

            ColumnNodeIDs iNodes;
            iNodes.push_back(interestI);

            // Look for any outgoing edges from Interest node
            for (const EdgeRecord& e2 : read().getOutEdges(&iNodes)) {
                const NodeID nodeX = e2._otherID;

                const auto* nameP =
                    read().tryGetNodeProperty<types::String>(nameID, personP);
                const auto* nameI =
                    read().tryGetNodeProperty<types::String>(nameID, interestI);
                const auto* nameX =
                    read().tryGetNodeProperty<types::String>(nameID, nodeX);
                if (nameP && nameI && nameX) {
                    expected.add({*nameP, *nameI, *nameX});
                }
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesP = findColumn(df, "p.name");
            if (namesP) {
                auto* pCol = namesP->as<ColumnOptVector<String>>();
                auto* iCol = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
                auto* xCol = findColumn(df, "x.name")->as<ColumnOptVector<String>>();
                if (pCol && iCol && xCol) {
                    for (size_t row = 0; row < pCol->size(); row++) {
                        if (pCol->at(row) && iCol->at(row) && xCol->at(row)) {
                            actual.add({*pCol->at(row), *iCol->at(row), *xCol->at(row)});
                        }
                    }
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 4: BACKWARD CHAIN PATTERNS (a)<--(b)<--(c)
// =============================================================================

// Test Interest <- Person <- (who knows them)
TEST_F(MultiHopPatternTest, backwardChain_interestFromPerson) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person)<-[:KNOWS_WELL]-(q)
        RETURN i.name, p.name, q.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const EdgeTypeID INTERESTED_IN_TYPEID = 1;
    const EdgeTypeID KNOWS_WELL_TYPEID = 0;

    Rows expected;
    {
        // Build: Person -> their interests
        std::map<NodeID, std::vector<NodeID>> personToInterests;
        // Build: Person -> who knows them (incoming KNOWS_WELL)
        std::map<NodeID, std::vector<NodeID>> personKnownBy;

        for (const EdgeRecord& e : read().scanOutEdges()) {
            const EdgeTypeID edgeType = read().getEdgeTypeID(e._edgeID);
            NodeView srcView = read().getNodeView(e._nodeID);
            NodeView dstView = read().getNodeView(e._otherID);
            if (edgeType == INTERESTED_IN_TYPEID &&
                srcView.labelset().hasLabel(personLabelID) && dstView.labelset().hasLabel(interestLabelID)) {
                personToInterests[e._nodeID].push_back(e._otherID);
            } else if (edgeType == KNOWS_WELL_TYPEID) {
                // e._nodeID knows e._otherID well, so e._otherID is known BY e._nodeID
                personKnownBy[e._otherID].push_back(e._nodeID);
            }
        }

        for (const auto& [personP, interests] : personToInterests) {
            auto knownByIt = personKnownBy.find(personP);
            if (knownByIt == personKnownBy.end()) continue;

            const auto* nameP = read().tryGetNodeProperty<types::String>(nameID, personP);
            if (!nameP) continue;

            for (const NodeID interestI : interests) {
                for (const NodeID nodeQ : knownByIt->second) {
                    const auto* nameI = read().tryGetNodeProperty<types::String>(nameID, interestI);
                    const auto* nameQ = read().tryGetNodeProperty<types::String>(nameID, nodeQ);
                    if (nameI && nameQ) {
                        expected.add({*nameI, *nameP, *nameQ});
                    }
                }
            }
        }
    }
    // Adam knows Remy, Remy knows Adam, Ghosts knows Remy
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesI = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            auto* namesP = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            auto* namesQ = findColumn(df, "q.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesI && namesP && namesQ);
            for (size_t row = 0; row < namesI->size(); row++) {
                if (namesI->at(row) && namesP->at(row) && namesQ->at(row)) {
                    actual.add({*namesI->at(row), *namesP->at(row), *namesQ->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test backward chain with filter on non-French persons
TEST_F(MultiHopPatternTest, backwardChain_withFilter) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person)<-[:KNOWS_WELL]-(q)
        WHERE NOT p.isFrench
        RETURN i.name, p.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const EdgeTypeID INTERESTED_IN_TYPEID = 1;
    const EdgeTypeID KNOWS_WELL_TYPEID = 0;

    Rows expected;
    {
        std::map<NodeID, std::vector<NodeID>> personToInterests;
        std::map<NodeID, std::vector<NodeID>> personKnownBy;

        for (const EdgeRecord& e : read().scanOutEdges()) {
            const EdgeTypeID edgeType = read().getEdgeTypeID(e._edgeID);
            NodeView srcView = read().getNodeView(e._nodeID);
            NodeView dstView = read().getNodeView(e._otherID);
            if (edgeType == INTERESTED_IN_TYPEID &&
                srcView.labelset().hasLabel(personLabelID) && dstView.labelset().hasLabel(interestLabelID)) {
                personToInterests[e._nodeID].push_back(e._otherID);
            } else if (edgeType == KNOWS_WELL_TYPEID) {
                personKnownBy[e._otherID].push_back(e._nodeID);
            }
        }

        for (const auto& [personP, interests] : personToInterests) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, personP);
            if (isFrench && *isFrench) continue; // Skip French

            auto knownByIt = personKnownBy.find(personP);
            if (knownByIt == personKnownBy.end()) continue;

            const auto* nameP = read().tryGetNodeProperty<types::String>(nameID, personP);
            if (!nameP) continue;

            for (const NodeID interestI : interests) {
                const auto* nameI = read().tryGetNodeProperty<types::String>(nameID, interestI);
                if (nameI) {
                    expected.add({*nameI, *nameP});
                }
            }
        }
    }
    // Non-French persons don't have incoming KNOWS_WELL edges in SimpleGraph
    // So this should be empty

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesI = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            auto* namesP = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesI && namesP);
            for (size_t row = 0; row < namesI->size(); row++) {
                if (namesI->at(row) && namesP->at(row)) {
                    actual.add({*namesI->at(row), *namesP->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 5: LABEL CONSTRAINTS
// =============================================================================

// Test with all nodes labeled
// DISABLED: Comparison of NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_labelConstraint_allNodesLabeled) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (p1:Person)-->(i:Interest)<--(p2:Person)
        WHERE p1 <> p2
        RETURN p1.name, i.name, p2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    {
        std::map<NodeID, std::vector<NodeID>> interestToPersons;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestToPersons[e._otherID].push_back(e._nodeID);
            }
        }

        for (const auto& [interestNode, persons] : interestToPersons) {
            const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, interestNode);
            if (!interestName) continue;

            for (size_t i = 0; i < persons.size(); i++) {
                for (size_t j = 0; j < persons.size(); j++) {
                    if (persons[i] != persons[j]) {
                        const auto* name1 = read().tryGetNodeProperty<types::String>(nameID, persons[i]);
                        const auto* name2 = read().tryGetNodeProperty<types::String>(nameID, persons[j]);
                        if (name1 && name2) {
                            expected.add({*name1, *interestName, *name2});
                        }
                    }
                }
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* names1 = findColumn(df, "p1.name")->as<ColumnOptVector<String>>();
            auto* namesI = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            auto* names2 = findColumn(df, "p2.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names1 && namesI && names2);
            for (size_t row = 0; row < names1->size(); row++) {
                if (names1->at(row) && namesI->at(row) && names2->at(row)) {
                    actual.add({*names1->at(row), *namesI->at(row), *names2->at(row)});
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test with only middle node labeled
// DISABLED: Comparison of NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_labelConstraint_middleNodeOnly) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a)-->(b:Interest)<--(c)
        WHERE a <> c
        RETURN DISTINCT b.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID interestLabelID = getLabelID("Interest");

    std::set<String> expectedInterests;
    {
        std::map<NodeID, std::set<NodeID>> interestSources;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestSources[e._otherID].insert(e._nodeID);
            }
        }

        for (const auto& [interestNode, sources] : interestSources) {
            if (sources.size() >= 2) {
                const auto* name = read().tryGetNodeProperty<types::String>(nameID, interestNode);
                if (name) {
                    expectedInterests.insert(*name);
                }
            }
        }
    }

    Rows expected;
    for (const auto& interest : expectedInterests) {
        expected.add({interest});
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);
            std::set<String> seen;
            for (size_t row = 0; row < names->size(); row++) {
                if (names->at(row) && seen.find(*names->at(row)) == seen.end()) {
                    actual.add({*names->at(row)});
                    seen.insert(*names->at(row));
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test with multiple labels on first node (Person:Founder)
// DISABLED: Comparison of NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_labelConstraint_multipleLabels) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (p:Person:Founder)-->(i:Interest)<--(q:Person)
        WHERE p <> q
        RETURN p.name, i.name, q.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID founderLabelID = getLabelID("Founder");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    {
        std::map<NodeID, std::vector<NodeID>> interestToPersons;
        std::map<NodeID, std::vector<NodeID>> interestToFounders;

        for (const EdgeRecord& e : read().scanOutEdges()) {
            NodeView srcView = read().getNodeView(e._nodeID);
            NodeView dstView = read().getNodeView(e._otherID);
            if (srcView.labelset().hasLabel(personLabelID) &&
                dstView.labelset().hasLabel(interestLabelID)) {
                interestToPersons[e._otherID].push_back(e._nodeID);
                if (srcView.labelset().hasLabel(founderLabelID)) {
                    interestToFounders[e._otherID].push_back(e._nodeID);
                }
            }
        }

        for (const auto& [interestNode, founders] : interestToFounders) {
            auto personsIt = interestToPersons.find(interestNode);
            if (personsIt == interestToPersons.end()) continue;

            const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, interestNode);
            if (!interestName) continue;

            for (const NodeID founder : founders) {
                for (const NodeID person : personsIt->second) {
                    if (founder != person) {
                        const auto* founderName = read().tryGetNodeProperty<types::String>(nameID, founder);
                        const auto* personName = read().tryGetNodeProperty<types::String>(nameID, person);
                        if (founderName && personName) {
                            expected.add({*founderName, *interestName, *personName});
                        }
                    }
                }
            }
        }
    }
    // Remy and Adam are Founders
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesP = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            auto* namesI = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            auto* namesQ = findColumn(df, "q.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesP && namesI && namesQ);
            for (size_t row = 0; row < namesP->size(); row++) {
                if (namesP->at(row) && namesI->at(row) && namesQ->at(row)) {
                    actual.add({*namesP->at(row), *namesI->at(row), *namesQ->at(row)});
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test edge and node labels together
TEST_F(MultiHopPatternTest, labelConstraint_edgeAndNodeLabels) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (p:Person)-[e:INTERESTED_IN]->(i:Interest)
        RETURN p.name, i.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const EdgeTypeID INTERESTED_IN_TYPEID = 1;

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e._edgeID) != INTERESTED_IN_TYPEID) continue;
            NodeView srcView = read().getNodeView(e._nodeID);
            NodeView dstView = read().getNodeView(e._otherID);
            if (!srcView.labelset().hasLabel(personLabelID) || !dstView.labelset().hasLabel(interestLabelID)) continue;

            const auto* personName = read().tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (personName && interestName) {
                expected.add({*personName, *interestName});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesP = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            auto* namesI = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesP && namesI);
            for (size_t row = 0; row < namesP->size(); row++) {
                if (namesP->at(row) && namesI->at(row)) {
                    actual.add({*namesP->at(row), *namesI->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 6: WHERE CLAUSE FILTERS
// =============================================================================

// Test filter on both ends - THE EXACT BUG SCENARIO
TEST_F(MultiHopPatternTest, whereFilter_booleanOnBothEnds) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (pf:Person)-->(i:Interest)<--(npf:Person)
        WHERE pf.isFrench AND NOT npf.isFrench
        RETURN pf.name, i.name, npf.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    {
        std::map<NodeID, std::vector<NodeID>> interestToFrench;
        std::map<NodeID, std::vector<NodeID>> interestToNonFrench;

        for (const EdgeRecord& e : read().scanOutEdges()) {
            NodeView srcView = read().getNodeView(e._nodeID);
            NodeView dstView = read().getNodeView(e._otherID);
            if (!srcView.labelset().hasLabel(personLabelID) || !dstView.labelset().hasLabel(interestLabelID)) continue;

            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, e._nodeID);
            if (isFrench && *isFrench) {
                interestToFrench[e._otherID].push_back(e._nodeID);
            } else if (isFrench && !*isFrench) {
                interestToNonFrench[e._otherID].push_back(e._nodeID);
            }
        }

        for (const auto& [interestNode, frenchPersons] : interestToFrench) {
            auto nonFrenchIt = interestToNonFrench.find(interestNode);
            if (nonFrenchIt == interestToNonFrench.end()) continue;

            const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, interestNode);
            if (!interestName) continue;

            for (const NodeID french : frenchPersons) {
                for (const NodeID nonFrench : nonFrenchIt->second) {
                    const auto* frenchName = read().tryGetNodeProperty<types::String>(nameID, french);
                    const auto* nonFrenchName = read().tryGetNodeProperty<types::String>(nameID, nonFrench);
                    if (frenchName && nonFrenchName) {
                        expected.add({*frenchName, *interestName, *nonFrenchName});
                    }
                }
            }
        }
    }
    // Expected: Adam -> Cooking <- Martina
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesPF = findColumn(df, "pf.name")->as<ColumnOptVector<String>>();
            auto* namesI = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            auto* namesNPF = findColumn(df, "npf.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesPF && namesI && namesNPF);
            for (size_t row = 0; row < namesPF->size(); row++) {
                if (namesPF->at(row) && namesI->at(row) && namesNPF->at(row)) {
                    actual.add({*namesPF->at(row), *namesI->at(row), *namesNPF->at(row)});
                }
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test string equality filter on middle node
// DISABLED: Comparison of NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_whereFilter_stringEquality) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.name = 'Cooking' AND a <> c
        RETURN a.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    {
        // Find Cooking interest
        for (const NodeID interestNode : read().scanNodes()) {
            if (!read().getNodeView(interestNode).labelset().hasLabel(interestLabelID)) continue;

            const auto* interestName = read().tryGetNodeProperty<types::String>(nameID, interestNode);
            if (!interestName || *interestName != "Cooking") continue;

            std::vector<NodeID> interestedPersons;
            for (const EdgeRecord& e : read().scanOutEdges()) {
                if (e._otherID != interestNode) continue;
                if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID)) {
                    interestedPersons.push_back(e._nodeID);
                }
            }

            for (size_t i = 0; i < interestedPersons.size(); i++) {
                for (size_t j = 0; j < interestedPersons.size(); j++) {
                    if (interestedPersons[i] != interestedPersons[j]) {
                        const auto* nameA = read().tryGetNodeProperty<types::String>(nameID, interestedPersons[i]);
                        const auto* nameC = read().tryGetNodeProperty<types::String>(nameID, interestedPersons[j]);
                        if (nameA && nameC) {
                            expected.add({*nameA, *nameC});
                        }
                    }
                }
            }
        }
    }
    // Expected: (Adam, Martina), (Martina, Adam)
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* namesA = findColumn(df, "a.name")->as<ColumnOptVector<String>>();
            auto* namesC = findColumn(df, "c.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(namesA && namesC);
            for (size_t row = 0; row < namesA->size(); row++) {
                if (namesA->at(row) && namesC->at(row)) {
                    actual.add({*namesA->at(row), *namesC->at(row)});
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 7: RETURN VARIATIONS
// =============================================================================

// Test returning node IDs
// DISABLED: Comparison of NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_returnVariation_nodeIds) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE a <> c
        RETURN a, b, c
    )";

    using Rows = LineContainer<NodeID, NodeID, NodeID>;

    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    {
        std::map<NodeID, std::vector<NodeID>> interestToPersons;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestToPersons[e._otherID].push_back(e._nodeID);
            }
        }

        for (const auto& [interestNode, persons] : interestToPersons) {
            for (size_t i = 0; i < persons.size(); i++) {
                for (size_t j = 0; j < persons.size(); j++) {
                    if (persons[i] != persons[j]) {
                        expected.add({persons[i], interestNode, persons[j]});
                    }
                }
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* nodesA = findColumn(df, "a")->as<ColumnNodeIDs>();
            auto* nodesB = findColumn(df, "b")->as<ColumnNodeIDs>();
            auto* nodesC = findColumn(df, "c")->as<ColumnNodeIDs>();
            ASSERT_TRUE(nodesA && nodesB && nodesC);
            for (size_t row = 0; row < nodesA->size(); row++) {
                actual.add({nodesA->at(row), nodesB->at(row), nodesC->at(row)});
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test DISTINCT on middle node
// DISABLED: Comparison of NodePattern not yet supported
TEST_F(MultiHopPatternTest, DISABLED_returnVariation_distinctMiddle) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE a <> c
        RETURN DISTINCT b.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    std::set<String> expectedInterests;
    {
        std::map<NodeID, int> interestPersonCount;
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getNodeView(e._nodeID).labelset().hasLabel(personLabelID) &&
                read().getNodeView(e._otherID).labelset().hasLabel(interestLabelID)) {
                interestPersonCount[e._otherID]++;
            }
        }

        for (const auto& [interestNode, count] : interestPersonCount) {
            if (count >= 2) {
                const auto* name = read().tryGetNodeProperty<types::String>(nameID, interestNode);
                if (name) {
                    expectedInterests.insert(*name);
                }
            }
        }
    }

    Rows expected;
    for (const auto& interest : expectedInterests) {
        expected.add({interest});
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "b.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);
            std::set<String> seen;
            for (size_t row = 0; row < names->size(); row++) {
                if (names->at(row) && seen.find(*names->at(row)) == seen.end()) {
                    actual.add({*names->at(row)});
                    seen.insert(*names->at(row));
                }
            }
        });
        ASSERT_FALSE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
