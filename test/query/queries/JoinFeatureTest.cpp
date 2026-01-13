#include <gtest/gtest.h>

#include <string_view>
#include <set>
#include <map>
#include <vector>

#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
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
// JOIN FEATURE TESTS
// Tests designed to find bugs and trigger segfaults in the join implementation.
// Key targets:
// - HashJoinProcessor.cpp line 203 bug (findInMap vs _rightMap.end())
// - MaterializeProcessor crashes with explicit edge types
// - Chunk boundary handling
// - Empty/null result handling
// =============================================================================

namespace {

// Custom test graph designed to stress-test join operations
class JoinTestGraph {
public:
    static void createJoinTestGraph(Graph* graph) {
        GraphWriter writer{graph};
        writer.setName("jointest");

        // =====================================================================
        // PERSONS: A, B, C, D, E, F
        // A, B: isFrench=true
        // C, D: isFrench=false
        // E: isFrench is NULL (missing property)
        // F: isolated (no edges)
        // =====================================================================
        auto A = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(A, "name", "A");
        writer.addNodeProperty<types::Bool>(A, "isFrench", true);

        auto B = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(B, "name", "B");
        writer.addNodeProperty<types::Bool>(B, "isFrench", true);

        auto C = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(C, "name", "C");
        writer.addNodeProperty<types::Bool>(C, "isFrench", false);

        auto D = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(D, "name", "D");
        writer.addNodeProperty<types::Bool>(D, "isFrench", false);

        auto E = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(E, "name", "E");
        // E has NO isFrench property - null case

        auto F = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(F, "name", "F");
        writer.addNodeProperty<types::Bool>(F, "isFrench", false);
        // F has no edges - isolated node for edge case testing

        // =====================================================================
        // INTERESTS: I1 (Shared), I2 (Unique), I3 (null name), I4 (Orphan), I5 (MegaHub)
        // I1: connected to A, B, C (3 persons - medium fan-in)
        // I2: connected only to A (single source)
        // I3: has NULL name property
        // I4: no incoming edges (orphan)
        // I5: connected to A, B, C, D, E (5 persons - high fan-in for stress test)
        // =====================================================================
        auto I1 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I1, "name", "Shared");

        auto I2 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I2, "name", "Unique");

        [[maybe_unused]] auto I3 = writer.addNode({"Interest"});
        // I3 has NO name property - null case for property filtering tests

        auto I4 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I4, "name", "Orphan");
        // I4 has no incoming edges - tests empty join results

        auto I5 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I5, "name", "MegaHub");

        // =====================================================================
        // CATEGORIES: Cat1
        // For multi-hop path testing: Person -> Interest -> Category
        // =====================================================================
        auto Cat1 = writer.addNode({"Category"});
        writer.addNodeProperty<types::String>(Cat1, "name", "Cat1");

        // =====================================================================
        // INTERESTED_IN edges: Person -> Interest
        // =====================================================================
        writer.addEdge("INTERESTED_IN", A, I1);  // A -> Shared
        writer.addEdge("INTERESTED_IN", B, I1);  // B -> Shared
        writer.addEdge("INTERESTED_IN", C, I1);  // C -> Shared
        writer.addEdge("INTERESTED_IN", A, I2);  // A -> Unique (only A)
        writer.addEdge("INTERESTED_IN", A, I5);  // A -> MegaHub
        writer.addEdge("INTERESTED_IN", B, I5);  // B -> MegaHub
        writer.addEdge("INTERESTED_IN", C, I5);  // C -> MegaHub
        writer.addEdge("INTERESTED_IN", D, I5);  // D -> MegaHub
        writer.addEdge("INTERESTED_IN", E, I5);  // E -> MegaHub

        // =====================================================================
        // KNOWS edges: Person -> Person (for self-join and cycle tests)
        // Pattern: A <-> B bidirectional, B -> C, C -> A (creates cycle)
        // =====================================================================
        writer.addEdge("KNOWS", A, B);  // A -> B
        writer.addEdge("KNOWS", B, A);  // B -> A (bidirectional)
        writer.addEdge("KNOWS", B, C);  // B -> C
        writer.addEdge("KNOWS", C, A);  // C -> A (creates cycle A->B->C->A)

        // =====================================================================
        // BELONGS_TO edges: Interest -> Category (for multi-hop)
        // =====================================================================
        writer.addEdge("BELONGS_TO", I1, Cat1);

        // =====================================================================
        // CITIES: City1 (Paris), City2 (London), City3 (Berlin)
        // For multi-join patterns: Person -> City, Interest -> City
        // =====================================================================
        auto City1 = writer.addNode({"City"});
        writer.addNodeProperty<types::String>(City1, "name", "Paris");

        auto City2 = writer.addNode({"City"});
        writer.addNodeProperty<types::String>(City2, "name", "London");

        auto City3 = writer.addNode({"City"});
        writer.addNodeProperty<types::String>(City3, "name", "Berlin");

        // =====================================================================
        // COMPANIES: Comp1, Comp2, Comp3
        // For multi-join patterns: Person -> Company, Company -> City
        // =====================================================================
        auto Comp1 = writer.addNode({"Company"});
        writer.addNodeProperty<types::String>(Comp1, "name", "TechCorp");

        auto Comp2 = writer.addNode({"Company"});
        writer.addNodeProperty<types::String>(Comp2, "name", "DataInc");

        auto Comp3 = writer.addNode({"Company"});
        writer.addNodeProperty<types::String>(Comp3, "name", "CloudLtd");

        // =====================================================================
        // MORE CATEGORIES: Cat2, Cat3 for multi-hop join tests
        // =====================================================================
        auto Cat2 = writer.addNode({"Category"});
        writer.addNodeProperty<types::String>(Cat2, "name", "Cat2");

        auto Cat3 = writer.addNode({"Category"});
        writer.addNodeProperty<types::String>(Cat3, "name", "Cat3");

        // =====================================================================
        // LIVES_IN edges: Person -> City
        // A, B -> Paris (shared city)
        // C, D -> London (shared city)
        // E -> Berlin (unique)
        // F -> no city (isolated)
        // =====================================================================
        writer.addEdge("LIVES_IN", A, City1);  // A -> Paris
        writer.addEdge("LIVES_IN", B, City1);  // B -> Paris
        writer.addEdge("LIVES_IN", C, City2);  // C -> London
        writer.addEdge("LIVES_IN", D, City2);  // D -> London
        writer.addEdge("LIVES_IN", E, City3);  // E -> Berlin

        // =====================================================================
        // WORKS_AT edges: Person -> Company
        // A -> TechCorp, B -> TechCorp (colleagues)
        // C -> DataInc, D -> DataInc (colleagues)
        // E -> CloudLtd
        // =====================================================================
        writer.addEdge("WORKS_AT", A, Comp1);  // A -> TechCorp
        writer.addEdge("WORKS_AT", B, Comp1);  // B -> TechCorp
        writer.addEdge("WORKS_AT", C, Comp2);  // C -> DataInc
        writer.addEdge("WORKS_AT", D, Comp2);  // D -> DataInc
        writer.addEdge("WORKS_AT", E, Comp3);  // E -> CloudLtd

        // =====================================================================
        // LOCATED_IN edges: Company -> City
        // TechCorp -> Paris
        // DataInc -> London
        // CloudLtd -> Berlin
        // =====================================================================
        writer.addEdge("LOCATED_IN", Comp1, City1);  // TechCorp -> Paris
        writer.addEdge("LOCATED_IN", Comp2, City2);  // DataInc -> London
        writer.addEdge("LOCATED_IN", Comp3, City3);  // CloudLtd -> Berlin

        // =====================================================================
        // Additional BELONGS_TO edges for multi-category tests
        // =====================================================================
        writer.addEdge("BELONGS_TO", I2, Cat2);  // Unique -> Cat2
        writer.addEdge("BELONGS_TO", I5, Cat2);  // MegaHub -> Cat2
        writer.addEdge("BELONGS_TO", I5, Cat3);  // MegaHub -> Cat3 (multi-category)

        // =====================================================================
        // HAS_EVENT edges: City -> Interest (events in cities)
        // For complex multi-hop: Person -> City -> Interest
        // =====================================================================
        writer.addEdge("HAS_EVENT", City1, I1);  // Paris has Shared event
        writer.addEdge("HAS_EVENT", City1, I5);  // Paris has MegaHub event
        writer.addEdge("HAS_EVENT", City2, I5);  // London has MegaHub event

        // =====================================================================
        // Additional KNOWS edges for more complex relationship patterns
        // =====================================================================
        writer.addEdge("KNOWS", A, C);  // A -> C (cross-city)
        writer.addEdge("KNOWS", D, E);  // D -> E (cross-city)
        writer.addEdge("KNOWS", C, D);  // C -> D (same city colleagues)

        writer.submit();
    }
};

} // namespace

class JoinFeatureTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path{_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        JoinTestGraph::createJoinTestGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "jointest";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db{nullptr};
    Graph* _graph{nullptr};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), ChangeID::head());
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

// =============================================================================
// CATEGORY 1: EMPTY/NULL JOIN CASES
// Tests for handling empty results gracefully without crashing
// =============================================================================

// Test 1: Left side returns no nodes (non-existent label)
// Note: Unknown labels cause query errors, not empty results
TEST_F(JoinFeatureTest, emptyLeftSideJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:NonExistentLabel)-->(b:Interest)<--(c:Person)
        RETURN a, b, c
    )";

    bool callbackCalled = false;
    auto res = query(QUERY, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_EQ(df->getRowCount(), 0);
        }
    });
    // Unknown labels cause query errors - this is expected behavior
    ASSERT_FALSE(res);
    EXPECT_FALSE(callbackCalled);
}

// Test 2: Right side returns no nodes (non-existent label)
// Note: Unknown labels cause query errors, not empty results
TEST_F(JoinFeatureTest, emptyRightSideJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:NonExistentLabel)
        RETURN a, b, c
    )";

    bool callbackCalled = false;
    auto res = query(QUERY, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_EQ(df->getRowCount(), 0);
        }
    });
    // Unknown labels cause query errors - this is expected behavior
    ASSERT_FALSE(res);
    EXPECT_FALSE(callbackCalled);
}

// Test 3: Both sides empty (all fake labels)
// Note: Unknown labels cause query errors, not empty results
TEST_F(JoinFeatureTest, bothSidesEmptyJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Fake1)-->(b:Fake2)<--(c:Fake3)
        RETURN a, b, c
    )";

    bool callbackCalled = false;
    auto res = query(QUERY, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_EQ(df->getRowCount(), 0);
        }
    });
    // Unknown labels cause query errors - this is expected behavior
    ASSERT_FALSE(res);
    EXPECT_FALSE(callbackCalled);
}

// Test 4: Join on orphan interest (I4 has no incoming edges)
TEST_F(JoinFeatureTest, joinOnOrphanNode) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.name = 'Orphan'
        RETURN a.name, b.name, c.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Orphan has no incoming edges, so join should produce 0 rows
    EXPECT_EQ(rowCount, 0);
}

// =============================================================================
// CATEGORY 3: HIGH CARDINALITY / STRESS TESTS
// These tests target the bug at HashJoinProcessor.cpp line 203
// =============================================================================

// Test 8: MegaHub N:M join (5 persons share MegaHub)
// CRITICAL: This test targets the bug at HashJoinProcessor.cpp:203
// Bug: findInMap(_leftMap, ...) compared against _rightMap.end()
TEST_F(JoinFeatureTest, megaHubJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.name = 'MegaHub' AND a.name <> c.name
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Find all Person nodes connected to the "MegaHub" interest
    std::vector<String> personsWithMegaHub;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        // Check if edge goes from Person to Interest named "MegaHub"
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (interestName && *interestName == "MegaHub") {
                const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
                if (personName) {
                    personsWithMegaHub.push_back(*personName);
                }
            }
        }
    }

    // Build expected results: all (a, "MegaHub", c) pairs where a != c
    Rows expected;
    for (const auto& a : personsWithMegaHub) {
        for (const auto& c : personsWithMegaHub) {
            if (a != c) {
                expected.add({a, String("MegaHub"), c});
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        auto* cNames = findColumn(df, "c.name");
        ASSERT_TRUE(aNames && bNames && cNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        auto* cCol = cNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol && cCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            if (aCol->at(i) && bCol->at(i) && cCol->at(i)) {
                actual.add({*aCol->at(i), *bCol->at(i), *cCol->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);
    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 9: Shared interest join (3 persons share I1)
TEST_F(JoinFeatureTest, sharedInterestJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.name = 'Shared' AND a.name <> c.name
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Find all Person nodes connected to the "Shared" interest
    std::vector<String> personsWithSharedInterest;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        // Check if edge goes from Person to Interest named "Shared"
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (interestName && *interestName == "Shared") {
                const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
                if (personName) {
                    personsWithSharedInterest.push_back(*personName);
                }
            }
        }
    }

    // Build expected results: all (a, "Shared", c) pairs where a != c
    Rows expected;
    for (const auto& a : personsWithSharedInterest) {
        for (const auto& c : personsWithSharedInterest) {
            if (a != c) {
                expected.add({a, String("Shared"), c});
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        auto* cNames = findColumn(df, "c.name");
        ASSERT_TRUE(aNames && bNames && cNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        auto* cCol = cNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol && cCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            if (aCol->at(i) && bCol->at(i) && cCol->at(i)) {
                actual.add({*aCol->at(i), *bCol->at(i), *cCol->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 10: Cartesian product - all persons with all interests
TEST_F(JoinFeatureTest, fullCartesianProduct) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person), (b:Interest)
        RETURN a.name, b.name
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Collect all person names
    std::vector<OptString> personNames;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (name) {
                personNames.push_back(*name);
            } else {
                personNames.push_back(std::nullopt);
            }
        }
    }

    // Collect all interest names (some may be null)
    std::vector<OptString> interestNames;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(interestLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (name) {
                interestNames.push_back(*name);
            } else {
                interestNames.push_back(std::nullopt);
            }
        }
    }

    // Build expected Cartesian product
    Rows expected;
    for (const auto& personName : personNames) {
        for (const auto& interestName : interestNames) {
            expected.add({personName, interestName});
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        ASSERT_TRUE(aNames && bNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            actual.add({aCol->at(i), bCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 11: Triple-hop join (Person -> Interest -> Category)
TEST_F(JoinFeatureTest, tripleHopJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)-->(c:Category)
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString, OptString>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Build (Person name, Interest NodeID) pairs
    std::vector<std::pair<OptString, NodeID>> personInterestEdges;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            personInterestEdges.push_back({
                personName ? std::optional<String>(*personName) : std::nullopt,
                e._otherID
            });
        }
    }

    // Build (Interest NodeID, Category name) pairs
    std::vector<std::pair<NodeID, OptString>> interestCategoryEdges;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(interestLabelID) &&
            dstView.labelset().hasLabel(categoryLabelID)) {
            const auto* categoryName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            interestCategoryEdges.push_back({
                e._nodeID,
                categoryName ? std::optional<String>(*categoryName) : std::nullopt
            });
        }
    }

    // Build expected results by joining on Interest NodeID
    Rows expected;
    for (const auto& [personName, interestID] : personInterestEdges) {
        const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, interestID);
        OptString intName = interestName ? std::optional<String>(*interestName) : std::nullopt;
        for (const auto& [icInterestID, categoryName] : interestCategoryEdges) {
            if (interestID == icInterestID) {
                expected.add({personName, intName, categoryName});
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        auto* cNames = findColumn(df, "c.name");
        ASSERT_TRUE(aNames && bNames && cNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        auto* cCol = cNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol && cCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            actual.add({aCol->at(i), bCol->at(i), cCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 4: EDGE TYPE & LABEL COMBINATIONS
// Tests for known crash patterns with explicit edge types
// =============================================================================

// Test 12: Explicit edge types - known to crash in MaterializeProcessor
TEST_F(JoinFeatureTest, explicitEdgeTypeCrash) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[e1:INTERESTED_IN]->(b:Interest)<-[e2:INTERESTED_IN]-(c:Person)
        WHERE a.name <> c.name
        RETURN a.name, b.name, c.name
    )";

    auto res = query(QUERY, [&](const Dataframe* df) {
        // If we get here without crashing, the bug is fixed!
        ASSERT_TRUE(df);
    });
    ASSERT_TRUE(res);
}

// Test 13: Mixed edge types in a path
TEST_F(JoinFeatureTest, mixedEdgeTypesInPath) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:INTERESTED_IN]->(c:Interest)
        RETURN a.name, b.name, c.name
    )";

    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        // A->B, B->A, B->C, C->A
        // A knows B, B knows {A, C}, C knows A
        // So:
        // A->B->MegaHub, A->B->Shared
        // B->A->MegaHub, B->A->Shared, B->A->Unique
        // B->C->MegaHub, B->C->Shared
        // C->A->MegaHub, C->A->Shared, C->A->Unique
    });
    ASSERT_TRUE(res);
}

// Test 14: Undirected edge join
TEST_F(JoinFeatureTest, undirectedEdgeJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)--(b:Person)
        RETURN a.name, b.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    // Build expected results using reader API
    // Undirected edge matches both directions of Person-to-Person edges (KNOWS)
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");

    Rows expected;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        // Only Person-Person edges (KNOWS edges in this graph)
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(personLabelID)) {
            const auto* srcName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* dstName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (srcName && dstName) {
                // Undirected matches both directions
                expected.add({*srcName, *dstName});
                expected.add({*dstName, *srcName});
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        ASSERT_TRUE(aNames && bNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            if (aCol->at(i) && bCol->at(i)) {
                actual.add({*aCol->at(i), *bCol->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 5: SELF-JOIN & CYCLE DETECTION
// Tests for cyclic patterns and self-referential joins
// =============================================================================

// Test 15: Self-join with equality filter (a = b)
// DISABLED: Comparison on NodePattern not yet supported
TEST_F(JoinFeatureTest, DISABLED_selfJoinWithEquality) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a = b
        RETURN a.name
    )";

    using String = types::String::Primitive;

    size_t rowCount = 0;
    std::set<String> foundNames;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
        auto* aNames = findColumn(df, "a.name");
        if (aNames) {
            auto* aCol = aNames->as<ColumnOptVector<String>>();
            if (aCol) {
                for (size_t i = 0; i < aCol->size(); i++) {
                    if (aCol->at(i)) foundNames.insert(*aCol->at(i));
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // No self-loops in graph, should return 0 rows
    EXPECT_EQ(rowCount, 0);
}

// Test 16: Cyclic pattern A->B->C->A
// DISABLED: Loops not yet supported
TEST_F(JoinFeatureTest, DISABLED_cyclicPatternJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a) RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;

    std::set<std::tuple<String, String, String>> foundCycles;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        auto* cNames = findColumn(df, "c.name");
        if (aNames && bNames && cNames) {
            auto* aCol = aNames->as<ColumnOptVector<String>>();
            auto* bCol = bNames->as<ColumnOptVector<String>>();
            auto* cCol = cNames->as<ColumnOptVector<String>>();
            if (aCol && bCol && cCol) {
                for (size_t i = 0; i < aCol->size(); i++) {
                    if (aCol->at(i) && bCol->at(i) && cCol->at(i)) {
                        foundCycles.insert({*aCol->at(i), *bCol->at(i), *cCol->at(i)});
                    }
                }
            }
        }
    });
    if (!res) {
        spdlog::error("{}", res.getError());
    }
    ASSERT_TRUE(res);
}

// Test 17: Symmetric relationship (A knows B AND B knows A)
// DISABLED: Loops not supported
TEST_F(JoinFeatureTest, DISABLED_symmetricRelationshipJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a)
        RETURN a.name, b.name
    )";

    using String = types::String::Primitive;

    std::set<std::pair<String, String>> foundPairs;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        if (aNames && bNames) {
            auto* aCol = aNames->as<ColumnOptVector<String>>();
            auto* bCol = bNames->as<ColumnOptVector<String>>();
            if (aCol && bCol) {
                for (size_t i = 0; i < aCol->size(); i++) {
                    if (aCol->at(i) && bCol->at(i)) {
                        foundPairs.insert({*aCol->at(i), *bCol->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // A <-> B is bidirectional
    // Expected: (A, B) and (B, A) - 2 rows
}

// =============================================================================
// CATEGORY 6: WHERE CLAUSE EDGE CASES
// Tests for filtering behavior with null properties and complex logic
// =============================================================================

// Test 18: Filter on null property (E has no isFrench)
TEST_F(JoinFeatureTest, nullPropertyFilter) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)
        WHERE a.isFrench = true
        RETURN a.name, b.name
    )";

    using String = types::String::Primitive;
    using OptString = std::optional<String>;
    using Rows = LineContainer<OptString, OptString>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Find all (Person name, Interest name) pairs where Person.isFrench = true
    Rows expected;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        // Person -> Interest edge
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* isFrench = reader.tryGetNodeProperty<types::Bool>(isFrenchID, e._nodeID);
            // Only include if isFrench is true
            if (isFrench && *isFrench) {
                const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
                const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
                expected.add({
                    personName ? std::optional<String>(*personName) : std::nullopt,
                    interestName ? std::optional<String>(*interestName) : std::nullopt
                });
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        ASSERT_TRUE(aNames && bNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            actual.add({aCol->at(i), bCol->at(i)});
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 19: Contradictory filter (always false)
TEST_F(JoinFeatureTest, contradictoryFilter) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)
        WHERE a.isFrench = true AND a.isFrench = false
        RETURN a, b
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Impossible condition, should return 0 rows
    EXPECT_EQ(rowCount, 0);
}

// Test 20: Complex boolean logic with XOR-like condition
TEST_F(JoinFeatureTest, complexBooleanFilter) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE a.isFrench = true AND c.isFrench = false AND a.name <> c.name
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // For each interest, collect French and non-French persons connected to it
    std::map<NodeID, std::vector<String>> frenchPersonsByInterest;
    std::map<NodeID, std::vector<String>> nonFrenchPersonsByInterest;
    std::map<NodeID, String> interestNames;

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        // Person -> Interest edge
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* isFrench = reader.tryGetNodeProperty<types::Bool>(isFrenchID, e._nodeID);
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);

            if (interestName) {
                interestNames[e._otherID] = *interestName;
            }

            if (personName && isFrench) {
                if (*isFrench) {
                    frenchPersonsByInterest[e._otherID].push_back(*personName);
                } else {
                    nonFrenchPersonsByInterest[e._otherID].push_back(*personName);
                }
            }
        }
    }

    // Build expected results: (french person, interest, non-french person) where a != c
    Rows expected;
    for (const auto& [interestID, frenchPersons] : frenchPersonsByInterest) {
        auto it = nonFrenchPersonsByInterest.find(interestID);
        if (it != nonFrenchPersonsByInterest.end()) {
            auto nameIt = interestNames.find(interestID);
            if (nameIt != interestNames.end()) {
                for (const auto& frenchPerson : frenchPersons) {
                    for (const auto& nonFrenchPerson : it->second) {
                        if (frenchPerson != nonFrenchPerson) {
                            expected.add({frenchPerson, nameIt->second, nonFrenchPerson});
                        }
                    }
                }
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        auto* cNames = findColumn(df, "c.name");
        ASSERT_TRUE(aNames && bNames && cNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        auto* cCol = cNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol && cCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            if (aCol->at(i) && bCol->at(i) && cCol->at(i)) {
                actual.add({*aCol->at(i), *bCol->at(i), *cCol->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 21: Filter with IS NULL (I3 has no name property)
TEST_F(JoinFeatureTest, filterOnNullProperty) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Interest)
        WHERE a.name IS NULL
        RETURN a
    )";

    // Compute expected count using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID interestLabelID = getLabelID("Interest");

    size_t expectedCount = 0;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(interestLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (!name) {
                // Interest with no name property (NULL)
                expectedCount++;
            }
        }
    }

    // Collect actual results
    size_t actualCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        actualCount += df->getRowCount();
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    EXPECT_EQ(actualCount, expectedCount);
}

// =============================================================================
// CATEGORY 7: CHUNK BOUNDARY TESTS
// Tests for correct handling of results spanning multiple chunks
// =============================================================================

// Test 22: Large result spanning multiple chunks
TEST_F(JoinFeatureTest, largeResultMultiChunk) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.name = 'MegaHub' AND a.name <> c.name
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API (same as megaHubJoin)
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    std::vector<String> personsWithMegaHub;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (interestName && *interestName == "MegaHub") {
                const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
                if (personName) {
                    personsWithMegaHub.push_back(*personName);
                }
            }
        }
    }

    Rows expected;
    for (const auto& a : personsWithMegaHub) {
        for (const auto& c : personsWithMegaHub) {
            if (a != c) {
                expected.add({a, String("MegaHub"), c});
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        if (aCol && bCol && cCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            if (a && b && c) {
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i) && c->at(i)) {
                        actual.add({*a->at(i), *b->at(i), *c->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 23: Empty result after processing
TEST_F(JoinFeatureTest, emptyAfterChunkProcessing) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.name = 'NonExistent' AND a.name <> c.name
        RETURN a.name, b.name, c.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    // No interest named 'NonExistent'
    // Expected: 0 rows, verify chunk state is clean
    EXPECT_EQ(rowCount, 0);
}

// =============================================================================
// ADDITIONAL STRESS TESTS
// Extra tests to maximize bug discovery potential
// =============================================================================

// Test 24: Very deep path (4 hops)
TEST_F(JoinFeatureTest, fourHopPath) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person)
        RETURN a.name, b.name, c.name, d.name
    )";

    auto res = query(QUERY, [&](const Dataframe* df) {
        // Deep path traversal stresses join operations
        ASSERT_TRUE(df);
    });
    ASSERT_TRUE(res);
}

// Test 25: Multiple disconnected patterns (cartesian product)
TEST_F(JoinFeatureTest, multipleDisconnectedPatterns) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person), (b:Person), (c:Person)
        WHERE a.name = 'A' AND b.name = 'B' AND c.name = 'C'
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    // Find persons A, B, C and verify they exist
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");

    bool foundA = false, foundB = false, foundC = false;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (name) {
                if (*name == "A") foundA = true;
                if (*name == "B") foundB = true;
                if (*name == "C") foundC = true;
            }
        }
    }

    // Build expected: should have exactly (A, B, C) if all three exist
    Rows expected;
    if (foundA && foundB && foundC) {
        expected.add({String("A"), String("B"), String("C")});
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        auto* cNames = findColumn(df, "c.name");
        ASSERT_TRUE(aNames && bNames && cNames);
        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        auto* cCol = cNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol && cCol);
        for (size_t i = 0; i < aCol->size(); i++) {
            if (aCol->at(i) && bCol->at(i) && cCol->at(i)) {
                actual.add({*aCol->at(i), *bCol->at(i), *cCol->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 26: Join with property projection
TEST_F(JoinFeatureTest, joinWithPropertyProjection) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)
        RETURN a.name, b.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    Rows expected;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (personName && interestName) {
                expected.add({*personName, *interestName});
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNameCol = findColumn(df, "a.name");
        auto* bNameCol = findColumn(df, "b.name");
        ASSERT_TRUE(aNameCol && bNameCol);
        auto* aName = aNameCol->as<ColumnOptVector<String>>();
        auto* bName = bNameCol->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aName && bName);
        for (size_t i = 0; i < aName->size(); i++) {
            if (aName->at(i) && bName->at(i)) {
                actual.add({*aName->at(i), *bName->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 27: Join with DISTINCT
// DISABLED: DISTINCT to yet supported.
TEST_F(JoinFeatureTest, DISABLED_joinWithDistinct) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE a.name <> c.name
        RETURN DISTINCT b.name
    )";

    using String = types::String::Primitive;

    std::set<String> distinctInterests;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* bNames = findColumn(df, "b.name");
        if (bNames) {
            auto* bCol = bNames->as<ColumnOptVector<String>>();
            if (bCol) {
                for (size_t i = 0; i < bCol->size(); i++) {
                    if (bCol->at(i)) {
                        distinctInterests.insert(*bCol->at(i));
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // Shared interests: Shared (A,B,C), MegaHub (A,B,C,D,E)
    // Expected: 2 distinct interests
}

// =============================================================================
// CATEGORY 8: COMPLEX MULTI-JOIN QUERIES
// Tests with multiple joins per query to stress test join processors
// Each test has 2+ join points in the query pattern
// Uses only supported syntax: chain patterns and implicit edge types
// =============================================================================

// Test 28: Double join chain - person to interest to person to interest
// Pattern: (a)-->(i1)<--(b)-->(i2)
// Joins: at i1 (a,b meet), continuation from b to i2
TEST_F(JoinFeatureTest, multiJoin_doubleInterestConnection) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)
        WHERE a.name <> b.name AND i1.name <> i2.name
        RETURN a.name, b.name, i1.name, i2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build person->interests map (with names)
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> personToInterests;
    std::map<NodeID, String> personNames;
    std::map<NodeID, String> interestNames;

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        // Person -> Interest edge
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (personName && interestName) {
                personNames[e._nodeID] = *personName;
                interestNames[e._otherID] = *interestName;
                personToInterests[e._nodeID].push_back({e._otherID, *interestName});
            }
        }
    }

    // Build interest->persons map (who is connected to each interest)
    std::map<NodeID, std::vector<NodeID>> interestToPersons;
    for (const auto& [personID, interests] : personToInterests) {
        for (const auto& [interestID, intName] : interests) {
            interestToPersons[interestID].push_back(personID);
        }
    }

    // Build expected results:
    // For each pair (a, b) who share interest i1 where a != b
    // Find all interests i2 that b has where i1 != i2
    Rows expected;
    for (const auto& [i1ID, persons] : interestToPersons) {
        auto i1NameIt = interestNames.find(i1ID);
        if (i1NameIt == interestNames.end()) continue;
        const String& i1Name = i1NameIt->second;

        for (NodeID aID : persons) {
            for (NodeID bID : persons) {
                auto aNameIt = personNames.find(aID);
                auto bNameIt = personNames.find(bID);
                if (aNameIt == personNames.end() || bNameIt == personNames.end()) continue;
                if (aNameIt->second == bNameIt->second) continue; // a.name != b.name

                // Find all interests i2 that b has where i1 != i2
                auto bInterestsIt = personToInterests.find(bID);
                if (bInterestsIt != personToInterests.end()) {
                    for (const auto& [i2ID, i2Name] : bInterestsIt->second) {
                        if (i1Name != i2Name) { // i1.name != i2.name
                            expected.add({aNameIt->second, bNameIt->second, i1Name, i2Name});
                        }
                    }
                }
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        ASSERT_TRUE(aCol && bCol && i1Col && i2Col);
        auto* a = aCol->as<ColumnOptVector<String>>();
        auto* b = bCol->as<ColumnOptVector<String>>();
        auto* i1 = i1Col->as<ColumnOptVector<String>>();
        auto* i2 = i2Col->as<ColumnOptVector<String>>();
        ASSERT_TRUE(a && b && i1 && i2);
        for (size_t i = 0; i < a->size(); i++) {
            if (a->at(i) && b->at(i) && i1->at(i) && i2->at(i)) {
                actual.add({*a->at(i), *b->at(i), *i1->at(i), *i2->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 29: Triple join chain - five nodes with three join points
// Pattern: (a)-->(i1)<--(b)-->(i2)<--(c)
// Joins: at i1, at i2 (two hash joins connecting three persons)
TEST_F(JoinFeatureTest, multiJoin_tripleJoinChain) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)<--(c:Person)
        WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name
        RETURN a.name, b.name, c.name, i1.name, i2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build person->interests and interest->persons maps
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> personToInterests;
    std::map<NodeID, std::vector<NodeID>> interestToPersons;
    std::map<NodeID, String> personNames;
    std::map<NodeID, String> interestNames;

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personNames[e._nodeID] = *pName;
                interestNames[e._otherID] = *iName;
                personToInterests[e._nodeID].push_back({e._otherID, *iName});
                interestToPersons[e._otherID].push_back(e._nodeID);
            }
        }
    }

    // Build expected: (a, b, c, i1, i2) where:
    // - a and b share i1 (a != b)
    // - b and c share i2 (b != c, a != c)
    Rows expected;
    for (const auto& [i1ID, i1Persons] : interestToPersons) {
        auto i1NameIt = interestNames.find(i1ID);
        if (i1NameIt == interestNames.end()) continue;

        for (NodeID aID : i1Persons) {
            for (NodeID bID : i1Persons) {
                auto aNameIt = personNames.find(aID);
                auto bNameIt = personNames.find(bID);
                if (aNameIt == personNames.end() || bNameIt == personNames.end()) continue;
                if (aNameIt->second == bNameIt->second) continue; // a != b

                // Find interests i2 that b has
                auto bInterestsIt = personToInterests.find(bID);
                if (bInterestsIt == personToInterests.end()) continue;

                for (const auto& [i2ID, i2Name] : bInterestsIt->second) {
                    // Find persons c connected to i2
                    auto i2PersonsIt = interestToPersons.find(i2ID);
                    if (i2PersonsIt == interestToPersons.end()) continue;

                    for (NodeID cID : i2PersonsIt->second) {
                        auto cNameIt = personNames.find(cID);
                        if (cNameIt == personNames.end()) continue;
                        if (bNameIt->second == cNameIt->second) continue; // b != c
                        if (aNameIt->second == cNameIt->second) continue; // a != c

                        expected.add({aNameIt->second, bNameIt->second, cNameIt->second,
                                      i1NameIt->second, i2Name});
                    }
                }
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        ASSERT_TRUE(aCol && bCol && cCol && i1Col && i2Col);
        auto* a = aCol->as<ColumnOptVector<String>>();
        auto* b = bCol->as<ColumnOptVector<String>>();
        auto* c = cCol->as<ColumnOptVector<String>>();
        auto* i1 = i1Col->as<ColumnOptVector<String>>();
        auto* i2 = i2Col->as<ColumnOptVector<String>>();
        ASSERT_TRUE(a && b && c && i1 && i2);
        for (size_t i = 0; i < a->size(); i++) {
            if (a->at(i) && b->at(i) && c->at(i) && i1->at(i) && i2->at(i)) {
                actual.add({*a->at(i), *b->at(i), *c->at(i), *i1->at(i), *i2->at(i)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 30: Long chain with cycle back - six nodes
// Pattern: (a)-->(i1)<--(b)-->(i2)<--(c)-->(i3)<--(a)
// Joins: at i1, i2, i3 (forms a cycle through interests)
TEST_F(JoinFeatureTest, multiJoin_interestCyclePattern) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)<--(c:Person)-->(i3:Interest)<--(a)
        WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name AND i1.name <> i2.name AND i2.name <> i3.name AND i1.name <> i3.name
        RETURN a.name, b.name, c.name, i1.name, i2.name, i3.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    // Loop pattern not supported - variable 'a' appears at both ends of chain
    ASSERT_FALSE(res);
}

// Test 31: Person chain to category - multi-hop with join
// Pattern: (a)-->(i)-->(cat)<--(j)<--(b)
// Joins: at interest level and category level
TEST_F(JoinFeatureTest, multiJoin_categoryBridge) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)-->(cat:Category)<--(j:Interest)<--(b:Person)
        WHERE a.name <> b.name AND i.name <> j.name
        RETURN a.name, b.name, i.name, j.name, cat.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Build maps
    std::map<NodeID, String> personNames;
    std::map<NodeID, String> interestNames;
    std::map<NodeID, String> categoryNames;
    std::map<NodeID, std::vector<NodeID>> personToInterests;  // person -> interests
    std::map<NodeID, std::vector<NodeID>> interestToCategories;  // interest -> categories
    std::map<NodeID, std::vector<NodeID>> categoryToInterests;  // category <- interests
    std::map<NodeID, std::vector<NodeID>> interestToPersons;  // interest <- persons

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);

        // Person -> Interest
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName) personNames[e._nodeID] = *pName;
            if (iName) interestNames[e._otherID] = *iName;
            personToInterests[e._nodeID].push_back(e._otherID);
            interestToPersons[e._otherID].push_back(e._nodeID);
        }
        // Interest -> Category
        if (srcView.labelset().hasLabel(interestLabelID) &&
            dstView.labelset().hasLabel(categoryLabelID)) {
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* cName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (iName) interestNames[e._nodeID] = *iName;
            if (cName) categoryNames[e._otherID] = *cName;
            interestToCategories[e._nodeID].push_back(e._otherID);
            categoryToInterests[e._otherID].push_back(e._nodeID);
        }
    }

    // Build expected: (a, b, i, j, cat) where:
    // a->i->cat<-j<-b, a != b, i != j
    Rows expected;
    for (const auto& [aID, aInterests] : personToInterests) {
        auto aNameIt = personNames.find(aID);
        if (aNameIt == personNames.end()) continue;

        for (NodeID iID : aInterests) {
            auto iNameIt = interestNames.find(iID);
            if (iNameIt == interestNames.end()) continue;

            auto iCatsIt = interestToCategories.find(iID);
            if (iCatsIt == interestToCategories.end()) continue;

            for (NodeID catID : iCatsIt->second) {
                auto catNameIt = categoryNames.find(catID);
                if (catNameIt == categoryNames.end()) continue;

                auto catInterestsIt = categoryToInterests.find(catID);
                if (catInterestsIt == categoryToInterests.end()) continue;

                for (NodeID jID : catInterestsIt->second) {
                    auto jNameIt = interestNames.find(jID);
                    if (jNameIt == interestNames.end()) continue;
                    if (iNameIt->second == jNameIt->second) continue; // i != j

                    auto jPersonsIt = interestToPersons.find(jID);
                    if (jPersonsIt == interestToPersons.end()) continue;

                    for (NodeID bID : jPersonsIt->second) {
                        auto bNameIt = personNames.find(bID);
                        if (bNameIt == personNames.end()) continue;
                        if (aNameIt->second == bNameIt->second) continue; // a != b

                        expected.add({aNameIt->second, bNameIt->second, iNameIt->second,
                                      jNameIt->second, catNameIt->second});
                    }
                }
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* iCol = findColumn(df, "i.name");
        auto* jCol = findColumn(df, "j.name");
        auto* catCol = findColumn(df, "cat.name");
        ASSERT_TRUE(aCol && bCol && iCol && jCol && catCol);
        auto* a = aCol->as<ColumnOptVector<String>>();
        auto* b = bCol->as<ColumnOptVector<String>>();
        auto* i = iCol->as<ColumnOptVector<String>>();
        auto* j = jCol->as<ColumnOptVector<String>>();
        auto* cat = catCol->as<ColumnOptVector<String>>();
        ASSERT_TRUE(a && b && i && j && cat);
        for (size_t idx = 0; idx < a->size(); idx++) {
            if (a->at(idx) && b->at(idx) && i->at(idx) && j->at(idx) && cat->at(idx)) {
                actual.add({*a->at(idx), *b->at(idx), *i->at(idx), *j->at(idx), *cat->at(idx)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 32: Three-way interest sharing - multiple persons on same interest chain
// Pattern: (a)-->(i)<--(b)<--(c) means: a->i, b->i, c->b (c KNOWS b)
// Joins: three persons meeting at same interest
TEST_F(JoinFeatureTest, multiJoin_threeWayInterestShare) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)<--(b:Person)<--(c:Person)
        WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name
        RETURN a.name, b.name, c.name, i.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build maps
    std::map<NodeID, String> personNames;
    std::map<NodeID, String> interestNames;
    std::map<NodeID, std::vector<NodeID>> interestToPersons;  // interest <- persons
    std::map<NodeID, std::vector<NodeID>> personToKnowers;    // person <- knowers (who knows this person)

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);

        // Person -> Interest
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName) personNames[e._nodeID] = *pName;
            if (iName) interestNames[e._otherID] = *iName;
            interestToPersons[e._otherID].push_back(e._nodeID);
        }
        // Person -> Person (KNOWS)
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(personLabelID)) {
            const auto* srcName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* dstName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (srcName) personNames[e._nodeID] = *srcName;
            if (dstName) personNames[e._otherID] = *dstName;
            personToKnowers[e._otherID].push_back(e._nodeID);  // src knows dst, so dst is known by src
        }
    }

    // Build expected: (a, b, c, i) where:
    // - a->i (a interested in i)
    // - b->i (b interested in i)
    // - c->b (c knows b)
    // - a != b, b != c, a != c
    Rows expected;
    for (const auto& [iID, iPersons] : interestToPersons) {
        auto iNameIt = interestNames.find(iID);
        if (iNameIt == interestNames.end()) continue;

        for (NodeID aID : iPersons) {
            auto aNameIt = personNames.find(aID);
            if (aNameIt == personNames.end()) continue;

            for (NodeID bID : iPersons) {
                auto bNameIt = personNames.find(bID);
                if (bNameIt == personNames.end()) continue;
                if (aNameIt->second == bNameIt->second) continue; // a != b

                // Find persons c who know b
                auto bKnowersIt = personToKnowers.find(bID);
                if (bKnowersIt == personToKnowers.end()) continue;

                for (NodeID cID : bKnowersIt->second) {
                    auto cNameIt = personNames.find(cID);
                    if (cNameIt == personNames.end()) continue;
                    if (bNameIt->second == cNameIt->second) continue; // b != c
                    if (aNameIt->second == cNameIt->second) continue; // a != c

                    expected.add({aNameIt->second, bNameIt->second, cNameIt->second, iNameIt->second});
                }
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        auto* iCol = findColumn(df, "i.name");
        ASSERT_TRUE(aCol && bCol && cCol && iCol);
        auto* a = aCol->as<ColumnOptVector<String>>();
        auto* b = bCol->as<ColumnOptVector<String>>();
        auto* c = cCol->as<ColumnOptVector<String>>();
        auto* i = iCol->as<ColumnOptVector<String>>();
        ASSERT_TRUE(a && b && c && i);
        for (size_t idx = 0; idx < a->size(); idx++) {
            if (a->at(idx) && b->at(idx) && c->at(idx) && i->at(idx)) {
                actual.add({*a->at(idx), *b->at(idx), *c->at(idx), *i->at(idx)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 33: Interest to category with extension
// Pattern: (a)-->(i)-->(cat), (b)-->(i)-->(cat)
// Chain pattern: (a)-->(i)-->(cat)<--(j)<--(b) where i might equal j
TEST_F(JoinFeatureTest, multiJoin_categoryJoinExtended) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)-->(cat:Category)<--(i)<--(b:Person)
        WHERE a.name <> b.name
        RETURN a.name, b.name, i.name, cat.name
    )";

    using String = types::String::Primitive;

    std::set<std::tuple<String, String, String, String>> results;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* iCol = findColumn(df, "i.name");
        auto* catCol = findColumn(df, "cat.name");
        if (aCol && bCol && iCol && catCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* interest = iCol->as<ColumnOptVector<String>>();
            auto* category = catCol->as<ColumnOptVector<String>>();
            if (a && b && interest && category) {
                for (size_t j = 0; j < a->size(); j++) {
                    if (a->at(j) && b->at(j) && interest->at(j) && category->at(j)) {
                        results.insert({*a->at(j), *b->at(j), *interest->at(j), *category->at(j)});
                    }
                }
            }
        }
    });
    // Loop pattern not supported - variable 'i' appears twice in chain
    ASSERT_FALSE(res);
}

// Test 34: Four-person chain through interests
// Pattern: (a)-->(i1)<--(b)-->(i2)<--(c)-->(i3)<--(d)
// Joins: at i1, i2, i3 (four persons connected)
TEST_F(JoinFeatureTest, multiJoin_fourPersonChain) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)<--(c:Person)-->(i3:Interest)<--(d:Person)
        WHERE a.name <> b.name AND b.name <> c.name AND c.name <> d.name AND a.name <> c.name AND a.name <> d.name AND b.name <> d.name
        RETURN a.name, b.name, c.name, d.name, i1.name, i2.name, i3.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build maps
    std::map<NodeID, String> personNames;
    std::map<NodeID, String> interestNames;
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> personToInterests;
    std::map<NodeID, std::vector<NodeID>> interestToPersons;

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personNames[e._nodeID] = *pName;
                interestNames[e._otherID] = *iName;
                personToInterests[e._nodeID].push_back({e._otherID, *iName});
                interestToPersons[e._otherID].push_back(e._nodeID);
            }
        }
    }

    // Build expected: (a, b, c, d, i1, i2, i3) where all persons distinct
    // a->i1<-b->i2<-c->i3<-d
    Rows expected;
    for (const auto& [i1ID, i1Persons] : interestToPersons) {
        auto i1NameIt = interestNames.find(i1ID);
        if (i1NameIt == interestNames.end()) continue;

        for (NodeID aID : i1Persons) {
            auto aNameIt = personNames.find(aID);
            if (aNameIt == personNames.end()) continue;

            for (NodeID bID : i1Persons) {
                auto bNameIt = personNames.find(bID);
                if (bNameIt == personNames.end()) continue;
                if (aNameIt->second == bNameIt->second) continue; // a != b

                // Find interests i2 that b has
                auto bInterestsIt = personToInterests.find(bID);
                if (bInterestsIt == personToInterests.end()) continue;

                for (const auto& [i2ID, i2Name] : bInterestsIt->second) {
                    auto i2PersonsIt = interestToPersons.find(i2ID);
                    if (i2PersonsIt == interestToPersons.end()) continue;

                    for (NodeID cID : i2PersonsIt->second) {
                        auto cNameIt = personNames.find(cID);
                        if (cNameIt == personNames.end()) continue;
                        if (bNameIt->second == cNameIt->second) continue; // b != c
                        if (aNameIt->second == cNameIt->second) continue; // a != c

                        // Find interests i3 that c has
                        auto cInterestsIt = personToInterests.find(cID);
                        if (cInterestsIt == personToInterests.end()) continue;

                        for (const auto& [i3ID, i3Name] : cInterestsIt->second) {
                            auto i3PersonsIt = interestToPersons.find(i3ID);
                            if (i3PersonsIt == interestToPersons.end()) continue;

                            for (NodeID dID : i3PersonsIt->second) {
                                auto dNameIt = personNames.find(dID);
                                if (dNameIt == personNames.end()) continue;
                                if (cNameIt->second == dNameIt->second) continue; // c != d
                                if (aNameIt->second == dNameIt->second) continue; // a != d
                                if (bNameIt->second == dNameIt->second) continue; // b != d

                                expected.add({aNameIt->second, bNameIt->second, cNameIt->second,
                                              dNameIt->second, i1NameIt->second, i2Name, i3Name});
                            }
                        }
                    }
                }
            }
        }
    }

    // Collect actual results
    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        auto* dCol = findColumn(df, "d.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* i3Col = findColumn(df, "i3.name");
        ASSERT_TRUE(aCol && bCol && cCol && dCol && i1Col && i2Col && i3Col);
        auto* a = aCol->as<ColumnOptVector<String>>();
        auto* b = bCol->as<ColumnOptVector<String>>();
        auto* c = cCol->as<ColumnOptVector<String>>();
        auto* d = dCol->as<ColumnOptVector<String>>();
        auto* i1 = i1Col->as<ColumnOptVector<String>>();
        auto* i2 = i2Col->as<ColumnOptVector<String>>();
        auto* i3 = i3Col->as<ColumnOptVector<String>>();
        ASSERT_TRUE(a && b && c && d && i1 && i2 && i3);
        for (size_t idx = 0; idx < a->size(); idx++) {
            if (a->at(idx) && b->at(idx) && c->at(idx) && d->at(idx) &&
                i1->at(idx) && i2->at(idx) && i3->at(idx)) {
                actual.add({*a->at(idx), *b->at(idx), *c->at(idx), *d->at(idx),
                            *i1->at(idx), *i2->at(idx), *i3->at(idx)});
            }
        }
    });
    ASSERT_TRUE(res);

    // Compare expected vs actual
    ASSERT_TRUE(expected.equals(actual));
}

// Test 35: Interest branching - one person to multiple interests joining
// Pattern: (a)-->(i1)<--(b)-->(i1) - same interest double join
TEST_F(JoinFeatureTest, multiJoin_sharedInterestDouble) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)<--(b:Person)-->(j:Interest)<--(a)
        WHERE a.name <> b.name AND i.name <> j.name
        RETURN a.name, b.name, i.name, j.name
    )";

    using String = types::String::Primitive;

    std::set<std::pair<String, String>> personPairs;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        if (aCol && bCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            if (a && b) {
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i)) {
                        personPairs.insert({*a->at(i), *b->at(i)});
                    }
                }
            }
        }
    });
    // Loop pattern not supported - variable 'a' appears at both ends of chain
    ASSERT_FALSE(res);
}

// Test 36: Category double hop
// Pattern: (a)-->(i)-->(c1)<--(j)-->(c2)
// Joins: at category c1, interest j, category c2
TEST_F(JoinFeatureTest, multiJoin_categoryDoubleHop) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)-->(c1:Category)<--(j:Interest)-->(c2:Category)
        WHERE c1.name <> c2.name
        RETURN a.name, i.name, j.name, c1.name, c2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Build person->interests map
    std::map<String, std::vector<std::pair<NodeID, String>>> personInterests;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personInterests[*pName].push_back({e._otherID, *iName});
            }
        }
    }

    // Build interest->categories map
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> interestCategories;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(interestLabelID) &&
            dstView.labelset().hasLabel(categoryLabelID)) {
            const auto* catName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (catName) {
                interestCategories[e._nodeID].push_back({e._otherID, *catName});
            }
        }
    }

    // Build category->interests map (interests that belong to each category)
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> categoryInterests;
    for (const auto& [iID, cats] : interestCategories) {
        const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, iID);
        if (iName) {
            for (const auto& [catID, catName] : cats) {
                categoryInterests[catID].push_back({iID, *iName});
            }
        }
    }

    // Pattern: (a)-->(i)-->(c1)<--(j)-->(c2) where c1.name <> c2.name
    Rows expected;
    for (const auto& [aName, aInterests] : personInterests) {
        for (const auto& [iID, iName] : aInterests) {
            // Find categories c1 that i belongs to
            auto iCatsIt = interestCategories.find(iID);
            if (iCatsIt == interestCategories.end()) continue;

            for (const auto& [c1ID, c1Name] : iCatsIt->second) {
                // Find interests j that belong to c1
                auto c1IntsIt = categoryInterests.find(c1ID);
                if (c1IntsIt == categoryInterests.end()) continue;

                for (const auto& [jID, jName] : c1IntsIt->second) {
                    // Note: No edge uniqueness in our Cypher - i and j can be the same node

                    // Find categories c2 that j belongs to where c1 != c2
                    auto jCatsIt = interestCategories.find(jID);
                    if (jCatsIt == interestCategories.end()) continue;

                    for (const auto& [c2ID, c2Name] : jCatsIt->second) {
                        if (c1Name != c2Name) {
                            expected.add({aName, iName, jName, c1Name, c2Name});
                        }
                    }
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* iCol = findColumn(df, "i.name");
        auto* jCol = findColumn(df, "j.name");
        auto* c1Col = findColumn(df, "c1.name");
        auto* c2Col = findColumn(df, "c2.name");
        if (aCol && iCol && jCol && c1Col && c2Col) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* i = iCol->as<ColumnOptVector<String>>();
            auto* j = jCol->as<ColumnOptVector<String>>();
            auto* c1 = c1Col->as<ColumnOptVector<String>>();
            auto* c2 = c2Col->as<ColumnOptVector<String>>();
            if (a && i && j && c1 && c2) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && i->at(idx) && j->at(idx) && c1->at(idx) && c2->at(idx)) {
                        actual.add({*a->at(idx), *i->at(idx), *j->at(idx), *c1->at(idx), *c2->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 37: Person chain with category endpoint
// Pattern: (a)-->(i1)<--(b)-->(i2)-->(cat)
// Joins: at i1, extends to i2 and category
TEST_F(JoinFeatureTest, multiJoin_personChainToCategory) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)-->(cat:Category)
        WHERE a.name <> b.name AND i1.name <> i2.name
        RETURN a.name, b.name, i1.name, i2.name, cat.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String>;

    // Build expected results using reader API
    // Pattern: (a)-->(i1)<--(b)-->(i2)-->(cat)
    // WHERE a.name <> b.name AND i1.name <> i2.name
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Build person->interests map
    std::map<String, std::vector<std::pair<NodeID, String>>> personInterests;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (personName && interestName) {
                personInterests[*personName].push_back({e._otherID, *interestName});
            }
        }
    }

    // Build interest->categories map
    std::map<NodeID, std::vector<String>> interestCategories;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(interestLabelID) &&
            dstView.labelset().hasLabel(categoryLabelID)) {
            const auto* catName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (catName) {
                interestCategories[e._nodeID].push_back(*catName);
            }
        }
    }

    Rows expected;
    // For each (a, i1) pair
    for (const auto& [aName, aInterests] : personInterests) {
        for (const auto& [i1ID, i1Name] : aInterests) {
            // Find all persons b who also have i1 (a <> b)
            for (const auto& [bName, bInterests] : personInterests) {
                if (aName == bName) continue;
                bool bHasI1 = false;
                for (const auto& [iID, iName] : bInterests) {
                    if (iID == i1ID) { bHasI1 = true; break; }
                }
                if (!bHasI1) continue;

                // For each i2 that b has (i1 <> i2)
                for (const auto& [i2ID, i2Name] : bInterests) {
                    if (i1Name == i2Name) continue;

                    // For each category that i2 belongs to
                    auto catIt = interestCategories.find(i2ID);
                    if (catIt != interestCategories.end()) {
                        for (const auto& catName : catIt->second) {
                            expected.add({aName, bName, i1Name, i2Name, catName});
                        }
                    }
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* catCol = findColumn(df, "cat.name");
        if (aCol && bCol && i1Col && i2Col && catCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            auto* cat = catCol->as<ColumnOptVector<String>>();
            if (a && b && i1 && i2 && cat) {
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i) && i1->at(i) && i2->at(i) && cat->at(i)) {
                        actual.add({*a->at(i), *b->at(i), *i1->at(i), *i2->at(i), *cat->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 38: Five-hop chain pattern
// Pattern: (a)-->(i1)<--(b)-->(i2)-->(cat)<--(i3)<--(c)
// Joins: at i1, i2, cat, i3 (four join points)
TEST_F(JoinFeatureTest, multiJoin_fiveHopChain) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)-->(cat:Category)<--(i3:Interest)<--(c:Person)
        WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name
        RETURN a.name, b.name, c.name, i1.name, i2.name, i3.name, cat.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Build maps
    std::map<String, std::vector<std::pair<NodeID, String>>> personInterests;
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> interestCategories;
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> categoryInterests;

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personInterests[*pName].push_back({e._otherID, *iName});
            }
        }
        if (srcView.labelset().hasLabel(interestLabelID) &&
            dstView.labelset().hasLabel(categoryLabelID)) {
            const auto* catName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            if (catName && iName) {
                interestCategories[e._nodeID].push_back({e._otherID, *catName});
                categoryInterests[e._otherID].push_back({e._nodeID, *iName});
            }
        }
    }

    // Build interest->persons map
    std::map<NodeID, std::vector<String>> interestPersons;
    for (const auto& [pName, interests] : personInterests) {
        for (const auto& [iID, iName] : interests) {
            interestPersons[iID].push_back(pName);
        }
    }

    // Pattern: (a)-->(i1)<--(b)-->(i2)-->(cat)<--(i3)<--(c)
    Rows expected;
    for (const auto& [aName, aInterests] : personInterests) {
        for (const auto& [i1ID, i1Name] : aInterests) {
            // Find persons b who share i1 with a (a <> b)
            auto i1PersonsIt = interestPersons.find(i1ID);
            if (i1PersonsIt == interestPersons.end()) continue;

            for (const auto& bName : i1PersonsIt->second) {
                if (aName == bName) continue;

                // Find interests i2 that b has
                auto bInterestsIt = personInterests.find(bName);
                if (bInterestsIt == personInterests.end()) continue;

                for (const auto& [i2ID, i2Name] : bInterestsIt->second) {
                    // Find categories cat that i2 belongs to
                    auto i2CatsIt = interestCategories.find(i2ID);
                    if (i2CatsIt == interestCategories.end()) continue;

                    for (const auto& [catID, catName] : i2CatsIt->second) {
                        // Find interests i3 that belong to cat
                        auto catIntsIt = categoryInterests.find(catID);
                        if (catIntsIt == categoryInterests.end()) continue;

                        for (const auto& [i3ID, i3Name] : catIntsIt->second) {
                            // Find persons c who have i3 (a <> c, b <> c)
                            auto i3PersonsIt = interestPersons.find(i3ID);
                            if (i3PersonsIt == interestPersons.end()) continue;

                            for (const auto& cName : i3PersonsIt->second) {
                                if (aName == cName || bName == cName) continue;
                                expected.add({aName, bName, cName, i1Name, i2Name, i3Name, catName});
                            }
                        }
                    }
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* i3Col = findColumn(df, "i3.name");
        auto* catCol = findColumn(df, "cat.name");
        if (aCol && bCol && cCol && i1Col && i2Col && i3Col && catCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            auto* i3 = i3Col->as<ColumnOptVector<String>>();
            auto* cat = catCol->as<ColumnOptVector<String>>();
            if (a && b && c && i1 && i2 && i3 && cat) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && b->at(idx) && c->at(idx) && i1->at(idx) &&
                        i2->at(idx) && i3->at(idx) && cat->at(idx)) {
                        actual.add({*a->at(idx), *b->at(idx), *c->at(idx),
                                    *i1->at(idx), *i2->at(idx), *i3->at(idx), *cat->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 39: Symmetric interest chain
// Pattern: (a)-->(i)<--(b)-->(i)<--(c) where same interest appears twice
// Tests variable reuse in chain
// DISABLED: Loops not yet supported
TEST_F(JoinFeatureTest, DISABLED_multiJoin_symmetricInterestChain) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)<--(b:Person)-->(i)<--(c:Person) WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name RETURN a.name, b.name, c.name, i.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    // Loop pattern not supported - variable 'i' appears twice in chain
    ASSERT_TRUE(res);
}

// Test 40: Category-interest-category chain
// Pattern: (cat1)<--(i)-->(cat2) with person attached
// (a)-->(i)-->(cat1), (i)-->(cat2)
TEST_F(JoinFeatureTest, multiJoin_multiCategoryInterest) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)-->(cat1:Category), (i)-->(cat2:Category)
        WHERE cat1.name <> cat2.name
        RETURN a.name, i.name, cat1.name, cat2.name
    )";

    using String = types::String::Primitive;

    std::set<std::tuple<String, String, String, String>> results;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* iCol = findColumn(df, "i.name");
        auto* cat1Col = findColumn(df, "cat1.name");
        auto* cat2Col = findColumn(df, "cat2.name");
        if (aCol && iCol && cat1Col && cat2Col) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* interest = iCol->as<ColumnOptVector<String>>();
            auto* cat1 = cat1Col->as<ColumnOptVector<String>>();
            auto* cat2 = cat2Col->as<ColumnOptVector<String>>();
            if (a && interest && cat1 && cat2) {
                for (size_t j = 0; j < a->size(); j++) {
                    if (a->at(j) && interest->at(j) && cat1->at(j) && cat2->at(j)) {
                        results.insert({*a->at(j), *interest->at(j), *cat1->at(j), *cat2->at(j)});
                    }
                }
            }
        }
    });
    // Loop pattern not supported - comma-separated patterns with shared variable 'i'
    ASSERT_FALSE(res);
}

// Test 41: Dual interest to single category
// Pattern: (a)-->(i1)-->(cat)<--(i2)<--(a) - person's two interests in same category
TEST_F(JoinFeatureTest, multiJoin_dualInterestSameCategory) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)-->(cat:Category)<--(i2:Interest)<--(a)
        WHERE i1.name <> i2.name
        RETURN a.name, i1.name, i2.name, cat.name
    )";

    using String = types::String::Primitive;

    std::set<std::tuple<String, String, String, String>> results;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* catCol = findColumn(df, "cat.name");
        if (aCol && i1Col && i2Col && catCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            auto* cat = catCol->as<ColumnOptVector<String>>();
            if (a && i1 && i2 && cat) {
                for (size_t j = 0; j < a->size(); j++) {
                    if (a->at(j) && i1->at(j) && i2->at(j) && cat->at(j)) {
                        results.insert({*a->at(j), *i1->at(j), *i2->at(j), *cat->at(j)});
                    }
                }
            }
        }
    });
    // Loop pattern not supported - variable 'a' appears at both ends of chain
    ASSERT_FALSE(res);
}

// Test 42: Extended person-interest-category chain
// Pattern: (a)-->(i1)<--(b)-->(i2)-->(cat)<--(i3)
TEST_F(JoinFeatureTest, multiJoin_extendedChainWithCategory) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)-->(cat:Category)<--(i3:Interest)
        WHERE a.name <> b.name AND i1.name <> i2.name AND i2.name <> i3.name
        RETURN a.name, b.name, i1.name, i2.name, i3.name, cat.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Build maps
    std::map<String, std::vector<std::pair<NodeID, String>>> personInterests;
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> interestCategories;
    std::map<NodeID, std::vector<std::pair<NodeID, String>>> categoryInterests;

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personInterests[*pName].push_back({e._otherID, *iName});
            }
        }
        if (srcView.labelset().hasLabel(interestLabelID) &&
            dstView.labelset().hasLabel(categoryLabelID)) {
            const auto* catName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            if (catName && iName) {
                interestCategories[e._nodeID].push_back({e._otherID, *catName});
                categoryInterests[e._otherID].push_back({e._nodeID, *iName});
            }
        }
    }

    // Build interest->persons map
    std::map<NodeID, std::vector<String>> interestPersons;
    for (const auto& [pName, interests] : personInterests) {
        for (const auto& [iID, iName] : interests) {
            interestPersons[iID].push_back(pName);
        }
    }

    // Pattern: (a)-->(i1)<--(b)-->(i2)-->(cat)<--(i3)
    // WHERE a.name <> b.name AND i1.name <> i2.name AND i2.name <> i3.name
    Rows expected;
    for (const auto& [aName, aInterests] : personInterests) {
        for (const auto& [i1ID, i1Name] : aInterests) {
            // Find persons b who share i1 with a (a <> b)
            auto i1PersonsIt = interestPersons.find(i1ID);
            if (i1PersonsIt == interestPersons.end()) continue;

            for (const auto& bName : i1PersonsIt->second) {
                if (aName == bName) continue;

                // Find interests i2 that b has (i1 <> i2)
                auto bInterestsIt = personInterests.find(bName);
                if (bInterestsIt == personInterests.end()) continue;

                for (const auto& [i2ID, i2Name] : bInterestsIt->second) {
                    if (i1Name == i2Name) continue;

                    // Find categories cat that i2 belongs to
                    auto i2CatsIt = interestCategories.find(i2ID);
                    if (i2CatsIt == interestCategories.end()) continue;

                    for (const auto& [catID, catName] : i2CatsIt->second) {
                        // Find interests i3 that belong to cat (i2 <> i3)
                        auto catIntsIt = categoryInterests.find(catID);
                        if (catIntsIt == categoryInterests.end()) continue;

                        for (const auto& [i3ID, i3Name] : catIntsIt->second) {
                            if (i2Name == i3Name) continue;
                            expected.add({aName, bName, i1Name, i2Name, i3Name, catName});
                        }
                    }
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* i3Col = findColumn(df, "i3.name");
        auto* catCol = findColumn(df, "cat.name");
        if (aCol && bCol && i1Col && i2Col && i3Col && catCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            auto* i3 = i3Col->as<ColumnOptVector<String>>();
            auto* cat = catCol->as<ColumnOptVector<String>>();
            if (a && b && i1 && i2 && i3 && cat) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && b->at(idx) && i1->at(idx) && i2->at(idx) &&
                        i3->at(idx) && cat->at(idx)) {
                        actual.add({*a->at(idx), *b->at(idx), *i1->at(idx),
                                    *i2->at(idx), *i3->at(idx), *cat->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 43: Three interests single category
// Pattern: (i1)-->(cat)<--(i2), (i3)-->(cat) - three interests in same category
TEST_F(JoinFeatureTest, multiJoin_threeInterestsSameCategory) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)-->(cat:Category)<--(i2:Interest)<--(b:Person)-->(i3:Interest)-->(cat)
        WHERE i1.name <> i2.name AND i2.name <> i3.name AND i1.name <> i3.name AND a.name <> b.name
        RETURN a.name, b.name, i1.name, i2.name, i3.name, cat.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    // Loop pattern not supported - variable 'cat' appears twice in chain
    ASSERT_FALSE(res);
}

// Test 44: Double person double interest chain
// Pattern: (a)-->(i1)<--(b)-->(i2)<--(a)-->(i3)<--(b)
// Two persons sharing three interests
TEST_F(JoinFeatureTest, multiJoin_doublePersonTripleInterest) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)<--(a)-->(i3:Interest)<--(b)
        WHERE a.name <> b.name AND i1.name <> i2.name AND i2.name <> i3.name AND i1.name <> i3.name
        RETURN a.name, b.name, i1.name, i2.name, i3.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    // Loop pattern not supported - variables 'a' and 'b' appear multiple times in chain
    ASSERT_FALSE(res);
}

// Test 45: Interest chain with property filter
// Pattern: Multi-hop with WHERE on node properties
TEST_F(JoinFeatureTest, multiJoin_chainWithPropertyFilter) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)<--(c:Person)
        WHERE a.isFrench = true AND c.isFrench = false AND a.name <> b.name AND b.name <> c.name AND a.name <> c.name
        RETURN a.name, b.name, c.name, i1.name, i2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String>;

    // Build expected results using reader API
    // Pattern: (a)-->(i1)<--(b)-->(i2)<--(c)
    // WHERE a.isFrench = true AND c.isFrench = false
    // AND a.name <> b.name AND b.name <> c.name AND a.name <> c.name
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build person info: name -> (interests, isFrench)
    struct PersonInfo {
        std::vector<std::pair<NodeID, String>> interests;
        std::optional<bool> isFrench;
    };
    std::map<String, PersonInfo> persons;

    // Scan for Person nodes and their isFrench property
    for (const auto& node : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(node);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, node);
            if (name) {
                const auto* isFrench = reader.tryGetNodeProperty<types::Bool>(isFrenchID, node);
                persons[*name].isFrench = isFrench ? std::optional<bool>(*isFrench) : std::nullopt;
            }
        }
    }

    // Build person->interests map
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (personName && interestName) {
                persons[*personName].interests.push_back({e._otherID, *interestName});
            }
        }
    }

    Rows expected;
    // For each person a where isFrench = true
    for (const auto& [aName, aInfo] : persons) {
        if (!aInfo.isFrench.has_value() || !aInfo.isFrench.value()) continue;

        for (const auto& [i1ID, i1Name] : aInfo.interests) {
            // Find all persons b who also have i1 (a <> b)
            for (const auto& [bName, bInfo] : persons) {
                if (aName == bName) continue;
                bool bHasI1 = false;
                for (const auto& [iID, iName] : bInfo.interests) {
                    if (iID == i1ID) { bHasI1 = true; break; }
                }
                if (!bHasI1) continue;

                // For each i2 that b has
                for (const auto& [i2ID, i2Name] : bInfo.interests) {
                    // Find all persons c who have i2 and c.isFrench = false
                    for (const auto& [cName, cInfo] : persons) {
                        if (bName == cName || aName == cName) continue;
                        if (!cInfo.isFrench.has_value() || cInfo.isFrench.value()) continue;

                        bool cHasI2 = false;
                        for (const auto& [iID, iName] : cInfo.interests) {
                            if (iID == i2ID) { cHasI2 = true; break; }
                        }
                        if (!cHasI2) continue;

                        expected.add({aName, bName, cName, i1Name, i2Name});
                    }
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        if (aCol && bCol && cCol && i1Col && i2Col) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            if (a && b && c && i1 && i2) {
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i) && c->at(i) && i1->at(i) && i2->at(i)) {
                        actual.add({*a->at(i), *b->at(i), *c->at(i), *i1->at(i), *i2->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 46: Six-node star from interest
// Pattern: Multiple persons connected to single interest, extended
// (a)-->(i)<--(b), (c)-->(i)<--(d)
TEST_F(JoinFeatureTest, multiJoin_interestStarPattern) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)<--(b:Person)-->(j:Interest)<--(c:Person)-->(i)
        WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name AND i.name <> j.name
        RETURN a.name, b.name, c.name, i.name, j.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    // Loop pattern not supported - variable 'i' appears twice in chain
    ASSERT_FALSE(res);
}

// Test 47: Maximum length chain - seven nodes
// Pattern: (a)-->(i1)<--(b)-->(i2)<--(c)-->(i3)<--(d)-->(i4)
TEST_F(JoinFeatureTest, multiJoin_sevenNodeChain) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest)<--(b:Person)-->(i2:Interest)<--(c:Person)-->(i3:Interest)<--(d:Person)-->(i4:Interest)
        WHERE a.name <> b.name AND b.name <> c.name AND c.name <> d.name AND a.name <> c.name AND a.name <> d.name AND b.name <> d.name
        RETURN a.name, b.name, c.name, d.name, i1.name, i2.name, i3.name, i4.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String, String, String, String>;

    // Build expected results using reader API
    // Pattern: (a)-->(i1)<--(b)-->(i2)<--(c)-->(i3)<--(d)-->(i4)
    // WHERE all four persons are distinct
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build person->interests map
    std::map<String, std::vector<std::pair<NodeID, String>>> personInterests;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* personName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* interestName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (personName && interestName) {
                personInterests[*personName].push_back({e._otherID, *interestName});
            }
        }
    }

    Rows expected;
    // Chain: a->i1<-b->i2<-c->i3<-d->i4
    for (const auto& [aName, aInterests] : personInterests) {
        for (const auto& [i1ID, i1Name] : aInterests) {
            // Find b who has i1 (a <> b)
            for (const auto& [bName, bInterests] : personInterests) {
                if (aName == bName) continue;
                bool bHasI1 = false;
                for (const auto& [iID, iName] : bInterests) {
                    if (iID == i1ID) { bHasI1 = true; break; }
                }
                if (!bHasI1) continue;

                // Find i2 that b has
                for (const auto& [i2ID, i2Name] : bInterests) {
                    // Find c who has i2 (a <> c, b <> c)
                    for (const auto& [cName, cInterests] : personInterests) {
                        if (aName == cName || bName == cName) continue;
                        bool cHasI2 = false;
                        for (const auto& [iID, iName] : cInterests) {
                            if (iID == i2ID) { cHasI2 = true; break; }
                        }
                        if (!cHasI2) continue;

                        // Find i3 that c has
                        for (const auto& [i3ID, i3Name] : cInterests) {
                            // Find d who has i3 (a <> d, b <> d, c <> d)
                            for (const auto& [dName, dInterests] : personInterests) {
                                if (aName == dName || bName == dName || cName == dName) continue;
                                bool dHasI3 = false;
                                for (const auto& [iID, iName] : dInterests) {
                                    if (iID == i3ID) { dHasI3 = true; break; }
                                }
                                if (!dHasI3) continue;

                                // Find i4 that d has
                                for (const auto& [i4ID, i4Name] : dInterests) {
                                    expected.add({aName, bName, cName, dName,
                                                  i1Name, i2Name, i3Name, i4Name});
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        auto* dCol = findColumn(df, "d.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* i3Col = findColumn(df, "i3.name");
        auto* i4Col = findColumn(df, "i4.name");
        if (aCol && bCol && cCol && dCol && i1Col && i2Col && i3Col && i4Col) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            auto* d = dCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            auto* i3 = i3Col->as<ColumnOptVector<String>>();
            auto* i4 = i4Col->as<ColumnOptVector<String>>();
            if (a && b && c && d && i1 && i2 && i3 && i4) {
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i) && c->at(i) && d->at(i) &&
                        i1->at(i) && i2->at(i) && i3->at(i) && i4->at(i)) {
                        actual.add({*a->at(i), *b->at(i), *c->at(i), *d->at(i),
                                    *i1->at(i), *i2->at(i), *i3->at(i), *i4->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 48: Eight-join chain - nine nodes with comprehensive WHERE predicates
// Pattern: (p1)-->(i1)<--(p2)-->(i2)<--(p3)-->(i3)<--(p4)-->(i4)<--(p5)
// This creates 8 join points connecting 5 persons through 4 interests
// No loop patterns - all variables are unique
TEST_F(JoinFeatureTest, multiJoin_eightJoinChain) {
    constexpr std::string_view QUERY = R"(
        MATCH (p1:Person)-->(i1:Interest)<--(p2:Person)-->(i2:Interest)<--(p3:Person)-->(i3:Interest)<--(p4:Person)-->(i4:Interest)<--(p5:Person)
        WHERE p1.name <> p2.name
          AND p2.name <> p3.name
          AND p3.name <> p4.name
          AND p4.name <> p5.name
          AND p1.name <> p3.name
          AND p1.name <> p4.name
          AND p1.name <> p5.name
          AND p2.name <> p4.name
          AND p2.name <> p5.name
          AND p3.name <> p5.name
          AND i1.name <> i2.name
          AND i2.name <> i3.name
          AND i3.name <> i4.name
          AND i1.name <> i3.name
          AND i1.name <> i4.name
          AND i2.name <> i4.name
        RETURN p1.name, p2.name, p3.name, p4.name, p5.name,
               i1.name, i2.name, i3.name, i4.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build person->interests map
    std::map<String, std::vector<std::pair<NodeID, String>>> personInterests;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personInterests[*pName].push_back({e._otherID, *iName});
            }
        }
    }

    // Build interest->persons map
    std::map<NodeID, std::vector<String>> interestPersons;
    for (const auto& [pName, interests] : personInterests) {
        for (const auto& [iID, iName] : interests) {
            interestPersons[iID].push_back(pName);
        }
    }

    // Helper to check all persons are distinct
    auto allPersonsDistinct = [](const String& p1, const String& p2, const String& p3,
                                  const String& p4, const String& p5) {
        return p1 != p2 && p1 != p3 && p1 != p4 && p1 != p5 &&
               p2 != p3 && p2 != p4 && p2 != p5 &&
               p3 != p4 && p3 != p5 &&
               p4 != p5;
    };

    // Helper to check all interests are distinct
    auto allInterestsDistinct = [](const String& i1, const String& i2,
                                    const String& i3, const String& i4) {
        return i1 != i2 && i1 != i3 && i1 != i4 &&
               i2 != i3 && i2 != i4 &&
               i3 != i4;
    };

    // Pattern: (p1)-->(i1)<--(p2)-->(i2)<--(p3)-->(i3)<--(p4)-->(i4)<--(p5)
    Rows expected;
    for (const auto& [p1Name, p1Interests] : personInterests) {
        for (const auto& [i1ID, i1Name] : p1Interests) {
            // Find p2 who shares i1 with p1
            auto i1PersonsIt = interestPersons.find(i1ID);
            if (i1PersonsIt == interestPersons.end()) continue;

            for (const auto& p2Name : i1PersonsIt->second) {
                if (p1Name == p2Name) continue;

                // Find i2 that p2 has (i1 != i2)
                auto p2InterestsIt = personInterests.find(p2Name);
                if (p2InterestsIt == personInterests.end()) continue;

                for (const auto& [i2ID, i2Name] : p2InterestsIt->second) {
                    if (i1Name == i2Name) continue;

                    // Find p3 who shares i2 with p2
                    auto i2PersonsIt = interestPersons.find(i2ID);
                    if (i2PersonsIt == interestPersons.end()) continue;

                    for (const auto& p3Name : i2PersonsIt->second) {
                        if (p1Name == p3Name || p2Name == p3Name) continue;

                        // Find i3 that p3 has (i1 != i3, i2 != i3)
                        auto p3InterestsIt = personInterests.find(p3Name);
                        if (p3InterestsIt == personInterests.end()) continue;

                        for (const auto& [i3ID, i3Name] : p3InterestsIt->second) {
                            if (i1Name == i3Name || i2Name == i3Name) continue;

                            // Find p4 who shares i3 with p3
                            auto i3PersonsIt = interestPersons.find(i3ID);
                            if (i3PersonsIt == interestPersons.end()) continue;

                            for (const auto& p4Name : i3PersonsIt->second) {
                                if (p1Name == p4Name || p2Name == p4Name || p3Name == p4Name) continue;

                                // Find i4 that p4 has (all interests distinct)
                                auto p4InterestsIt = personInterests.find(p4Name);
                                if (p4InterestsIt == personInterests.end()) continue;

                                for (const auto& [i4ID, i4Name] : p4InterestsIt->second) {
                                    if (!allInterestsDistinct(i1Name, i2Name, i3Name, i4Name)) continue;

                                    // Find p5 who shares i4 with p4 (all persons distinct)
                                    auto i4PersonsIt = interestPersons.find(i4ID);
                                    if (i4PersonsIt == interestPersons.end()) continue;

                                    for (const auto& p5Name : i4PersonsIt->second) {
                                        if (!allPersonsDistinct(p1Name, p2Name, p3Name, p4Name, p5Name)) continue;
                                        expected.add({p1Name, p2Name, p3Name, p4Name, p5Name,
                                                      i1Name, i2Name, i3Name, i4Name});
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* p1Col = findColumn(df, "p1.name");
        auto* p2Col = findColumn(df, "p2.name");
        auto* p3Col = findColumn(df, "p3.name");
        auto* p4Col = findColumn(df, "p4.name");
        auto* p5Col = findColumn(df, "p5.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* i3Col = findColumn(df, "i3.name");
        auto* i4Col = findColumn(df, "i4.name");

        if (p1Col && p2Col && p3Col && p4Col && p5Col &&
            i1Col && i2Col && i3Col && i4Col) {
            auto* p1 = p1Col->as<ColumnOptVector<String>>();
            auto* p2 = p2Col->as<ColumnOptVector<String>>();
            auto* p3 = p3Col->as<ColumnOptVector<String>>();
            auto* p4 = p4Col->as<ColumnOptVector<String>>();
            auto* p5 = p5Col->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            auto* i3 = i3Col->as<ColumnOptVector<String>>();
            auto* i4 = i4Col->as<ColumnOptVector<String>>();

            if (p1 && p2 && p3 && p4 && p5 && i1 && i2 && i3 && i4) {
                for (size_t idx = 0; idx < p1->size(); idx++) {
                    if (p1->at(idx) && p2->at(idx) && p3->at(idx) && p4->at(idx) && p5->at(idx) &&
                        i1->at(idx) && i2->at(idx) && i3->at(idx) && i4->at(idx)) {
                        actual.add({*p1->at(idx), *p2->at(idx), *p3->at(idx), *p4->at(idx), *p5->at(idx),
                                    *i1->at(idx), *i2->at(idx), *i3->at(idx), *i4->at(idx)});
                    }
                }
            }
        }
    });

    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// =============================================================================
// CATEGORY 9: COMMA-SEPARATED MATCH PATTERNS
// Tests using multiple match targets separated by commas (Cartesian products)
// =============================================================================

// Test 49: Two-way Cartesian - Person and Interest with property filter
TEST_F(JoinFeatureTest, commaMatch_twoWayCartesian) {
    constexpr std::string_view QUERY = R"(
        MATCH (p:Person), (i:Interest)
        WHERE p.isFrench = true AND i.name = 'MegaHub'
        RETURN p.name, i.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Find French persons and MegaHub interest
    std::vector<String> frenchPersons;
    String megaHubName;
    bool foundMegaHub = false;

    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* isFrench = reader.tryGetNodeProperty<types::Bool>(isFrenchID, nodeID);
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (isFrench && *isFrench && name) {
                frenchPersons.push_back(*name);
            }
        }
        if (nodeView.labelset().hasLabel(interestLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (name && *name == "MegaHub") {
                megaHubName = *name;
                foundMegaHub = true;
            }
        }
    }

    Rows expected;
    if (foundMegaHub) {
        for (const auto& pName : frenchPersons) {
            expected.add({pName, megaHubName});
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* pCol = findColumn(df, "p.name");
        auto* iCol = findColumn(df, "i.name");
        if (pCol && iCol) {
            auto* p = pCol->as<ColumnOptVector<String>>();
            auto* i = iCol->as<ColumnOptVector<String>>();
            if (p && i) {
                for (size_t idx = 0; idx < p->size(); idx++) {
                    if (p->at(idx) && i->at(idx)) {
                        actual.add({*p->at(idx), *i->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 50: Three-way Cartesian - Person, Interest, Category
TEST_F(JoinFeatureTest, commaMatch_threeWayCartesian) {
    constexpr std::string_view QUERY = R"(
        MATCH (p:Person), (i:Interest), (c:Category)
        WHERE p.name = 'A' AND i.name = 'Shared' AND c.name = 'Cat1'
        RETURN p.name, i.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Find if Person "A", Interest "Shared", and Category "Cat1" all exist
    bool foundPersonA = false;
    bool foundInterestShared = false;
    bool foundCategoryCat1 = false;

    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
        if (!name) continue;

        if (nodeView.labelset().hasLabel(personLabelID) && *name == "A") {
            foundPersonA = true;
        }
        if (nodeView.labelset().hasLabel(interestLabelID) && *name == "Shared") {
            foundInterestShared = true;
        }
        if (nodeView.labelset().hasLabel(categoryLabelID) && *name == "Cat1") {
            foundCategoryCat1 = true;
        }
    }

    Rows expected;
    // Cartesian product: if all three exist, there's exactly 1 combination
    if (foundPersonA && foundInterestShared && foundCategoryCat1) {
        expected.add({String("A"), String("Shared"), String("Cat1")});
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* pCol = findColumn(df, "p.name");
        auto* iCol = findColumn(df, "i.name");
        auto* cCol = findColumn(df, "c.name");
        if (pCol && iCol && cCol) {
            auto* p = pCol->as<ColumnOptVector<String>>();
            auto* i = iCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            if (p && i && c) {
                for (size_t idx = 0; idx < p->size(); idx++) {
                    if (p->at(idx) && i->at(idx) && c->at(idx)) {
                        actual.add({*p->at(idx), *i->at(idx), *c->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 51: Path pattern combined with single node (comma-separated)
TEST_F(JoinFeatureTest, commaMatch_pathWithSingleNode) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest), (c:Category)
        WHERE c.name = 'Cat1'
        RETURN a.name, i.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Find all Person->Interest pairs
    std::vector<std::pair<String, String>> personInterestPairs;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personInterestPairs.push_back({*pName, *iName});
            }
        }
    }

    // Find Cat1 category
    String cat1Name;
    bool foundCat1 = false;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(categoryLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (name && *name == "Cat1") {
                cat1Name = *name;
                foundCat1 = true;
                break;
            }
        }
    }

    Rows expected;
    if (foundCat1) {
        for (const auto& [pName, iName] : personInterestPairs) {
            expected.add({pName, iName, cat1Name});
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* iCol = findColumn(df, "i.name");
        auto* cCol = findColumn(df, "c.name");
        if (aCol && iCol && cCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* i = iCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            if (a && i && c) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && i->at(idx) && c->at(idx)) {
                        actual.add({*a->at(idx), *i->at(idx), *c->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 52: Two independent path patterns (comma-separated)
TEST_F(JoinFeatureTest, commaMatch_twoIndependentPaths) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest), (b:Person)-->(i2:Interest)
        WHERE a.name = 'A' AND b.name = 'B' AND i1.name <> i2.name
        RETURN a.name, b.name, i1.name, i2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Find interests for A and B
    std::vector<String> aInterests, bInterests;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                if (*pName == "A") aInterests.push_back(*iName);
                if (*pName == "B") bInterests.push_back(*iName);
            }
        }
    }

    // Build expected: (A, B, i1, i2) where i1 <> i2
    Rows expected;
    for (const auto& i1Name : aInterests) {
        for (const auto& i2Name : bInterests) {
            if (i1Name != i2Name) {
                expected.add({String("A"), String("B"), i1Name, i2Name});
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        if (aCol && bCol && i1Col && i2Col) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            if (a && b && i1 && i2) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && b->at(idx) && i1->at(idx) && i2->at(idx)) {
                        actual.add({*a->at(idx), *b->at(idx), *i1->at(idx), *i2->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 53: Four-way Cartesian with all different types
TEST_F(JoinFeatureTest, commaMatch_fourWayCartesian) {
    constexpr std::string_view QUERY = R"(
        MATCH (p:Person), (i:Interest), (c:Category), (city:City)
        WHERE p.name = 'A' AND i.name = 'Shared' AND c.name = 'Cat1' AND city.name = 'Paris'
        RETURN p.name, i.name, c.name, city.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");
    const LabelID cityLabelID = getLabelID("City");

    // Find if Person "A", Interest "Shared", Category "Cat1", and City "Paris" all exist
    bool foundPersonA = false;
    bool foundInterestShared = false;
    bool foundCategoryCat1 = false;
    bool foundCityParis = false;

    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
        if (!name) continue;

        if (nodeView.labelset().hasLabel(personLabelID) && *name == "A") {
            foundPersonA = true;
        }
        if (nodeView.labelset().hasLabel(interestLabelID) && *name == "Shared") {
            foundInterestShared = true;
        }
        if (nodeView.labelset().hasLabel(categoryLabelID) && *name == "Cat1") {
            foundCategoryCat1 = true;
        }
        if (nodeView.labelset().hasLabel(cityLabelID) && *name == "Paris") {
            foundCityParis = true;
        }
    }

    Rows expected;
    // Cartesian product: if all four exist, there's exactly 1 combination
    if (foundPersonA && foundInterestShared && foundCategoryCat1 && foundCityParis) {
        expected.add({String("A"), String("Shared"), String("Cat1"), String("Paris")});
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* pCol = findColumn(df, "p.name");
        auto* iCol = findColumn(df, "i.name");
        auto* cCol = findColumn(df, "c.name");
        auto* cityCol = findColumn(df, "city.name");
        if (pCol && iCol && cCol && cityCol) {
            auto* p = pCol->as<ColumnOptVector<String>>();
            auto* i = iCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            auto* city = cityCol->as<ColumnOptVector<String>>();
            if (p && i && c && city) {
                for (size_t idx = 0; idx < p->size(); idx++) {
                    if (p->at(idx) && i->at(idx) && c->at(idx) && city->at(idx)) {
                        actual.add({*p->at(idx), *i->at(idx), *c->at(idx), *city->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 54: Cartesian with multiple inequality filters
TEST_F(JoinFeatureTest, commaMatch_cartesianWithInequalities) {
    constexpr std::string_view QUERY = R"(
        MATCH (p1:Person), (p2:Person), (p3:Person)
        WHERE p1.name <> p2.name AND p2.name <> p3.name AND p1.name <> p3.name
          AND p1.isFrench = true
        RETURN p1.name, p2.name, p3.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");

    std::vector<String> frenchPersons;
    std::vector<String> allPersons;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            const auto* isFrench = reader.tryGetNodeProperty<types::Bool>(isFrenchID, nodeID);
            if (name) {
                allPersons.push_back(*name);
                if (isFrench && *isFrench) {
                    frenchPersons.push_back(*name);
                }
            }
        }
    }

    Rows expected;
    for (const auto& p1Name : frenchPersons) {
        for (const auto& p2Name : allPersons) {
            if (p1Name == p2Name) continue;
            for (const auto& p3Name : allPersons) {
                if (p2Name == p3Name || p1Name == p3Name) continue;
                expected.add({p1Name, p2Name, p3Name});
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* p1Col = findColumn(df, "p1.name");
        auto* p2Col = findColumn(df, "p2.name");
        auto* p3Col = findColumn(df, "p3.name");
        if (p1Col && p2Col && p3Col) {
            auto* p1 = p1Col->as<ColumnOptVector<String>>();
            auto* p2 = p2Col->as<ColumnOptVector<String>>();
            auto* p3 = p3Col->as<ColumnOptVector<String>>();
            if (p1 && p2 && p3) {
                for (size_t idx = 0; idx < p1->size(); idx++) {
                    if (p1->at(idx) && p2->at(idx) && p3->at(idx)) {
                        actual.add({*p1->at(idx), *p2->at(idx), *p3->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 55: Path and Cartesian combination with shared type
TEST_F(JoinFeatureTest, commaMatch_pathAndCartesianMixed) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)-->(c:Category), (other:Person)
        WHERE a.name <> other.name AND c.name = 'Cat1'
        RETURN a.name, i.name, c.name, other.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Find all persons
    std::vector<String> allPersons;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            if (name) allPersons.push_back(*name);
        }
    }

    // Find Person->Interest->Cat1 paths
    std::vector<std::pair<String, String>> personInterestToCat1;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                // Check if interest belongs to Cat1
                for (const auto& e2 : reader.scanOutEdges()) {
                    if (e2._nodeID == e._otherID) {
                        NodeView dstView2 = reader.getNodeView(e2._otherID);
                        if (dstView2.labelset().hasLabel(categoryLabelID)) {
                            const auto* catName = reader.tryGetNodeProperty<types::String>(nameID, e2._otherID);
                            if (catName && *catName == "Cat1") {
                                personInterestToCat1.push_back({*pName, *iName});
                            }
                        }
                    }
                }
            }
        }
    }

    Rows expected;
    for (const auto& [aName, iName] : personInterestToCat1) {
        for (const auto& otherName : allPersons) {
            if (aName != otherName) {
                expected.add({aName, iName, String("Cat1"), otherName});
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* iCol = findColumn(df, "i.name");
        auto* cCol = findColumn(df, "c.name");
        auto* oCol = findColumn(df, "other.name");
        if (aCol && iCol && cCol && oCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* i = iCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            auto* o = oCol->as<ColumnOptVector<String>>();
            if (a && i && c && o) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && i->at(idx) && c->at(idx) && o->at(idx)) {
                        actual.add({*a->at(idx), *i->at(idx), *c->at(idx), *o->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 56: Two paths with property-based join condition
TEST_F(JoinFeatureTest, commaMatch_twoPathsPropertyJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest), (b:Person)-->(i2:Interest)
        WHERE a.isFrench = true AND b.isFrench = false
          AND a.name <> b.name
        RETURN a.name, b.name, i1.name, i2.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Build person->interests map with isFrench info
    std::map<String, std::pair<std::optional<bool>, std::vector<String>>> personInfo;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            const auto* isFrench = reader.tryGetNodeProperty<types::Bool>(isFrenchID, nodeID);
            if (name) {
                personInfo[*name].first = isFrench ? std::optional<bool>(*isFrench) : std::nullopt;
            }
        }
    }

    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                personInfo[*pName].second.push_back(*iName);
            }
        }
    }

    Rows expected;
    for (const auto& [aName, aInfo] : personInfo) {
        if (!aInfo.first.has_value() || !aInfo.first.value()) continue; // a.isFrench = true
        for (const auto& [bName, bInfo] : personInfo) {
            if (!bInfo.first.has_value() || bInfo.first.value()) continue; // b.isFrench = false
            if (aName == bName) continue;
            for (const auto& i1Name : aInfo.second) {
                for (const auto& i2Name : bInfo.second) {
                    expected.add({aName, bName, i1Name, i2Name});
                }
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        if (aCol && bCol && i1Col && i2Col) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            if (a && b && i1 && i2) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && b->at(idx) && i1->at(idx) && i2->at(idx)) {
                        actual.add({*a->at(idx), *b->at(idx), *i1->at(idx), *i2->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 57: Three paths combined (stress test for comma patterns)
TEST_F(JoinFeatureTest, commaMatch_threePathsCombined) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest), (b:Person)-->(i2:Interest), (c:Person)-->(i3:Interest)
        WHERE a.name = 'A' AND b.name = 'B' AND c.name = 'C'
          AND i1.name <> i2.name AND i2.name <> i3.name AND i1.name <> i3.name
        RETURN a.name, b.name, c.name, i1.name, i2.name, i3.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String, String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");

    // Find interests for A, B, C
    std::vector<String> aInterests, bInterests, cInterests;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(personLabelID) &&
            dstView.labelset().hasLabel(interestLabelID)) {
            const auto* pName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (pName && iName) {
                if (*pName == "A") aInterests.push_back(*iName);
                if (*pName == "B") bInterests.push_back(*iName);
                if (*pName == "C") cInterests.push_back(*iName);
            }
        }
    }

    Rows expected;
    for (const auto& i1Name : aInterests) {
        for (const auto& i2Name : bInterests) {
            if (i1Name == i2Name) continue;
            for (const auto& i3Name : cInterests) {
                if (i2Name == i3Name || i1Name == i3Name) continue;
                expected.add({String("A"), String("B"), String("C"), i1Name, i2Name, i3Name});
            }
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* cCol = findColumn(df, "c.name");
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        auto* i3Col = findColumn(df, "i3.name");
        if (aCol && bCol && cCol && i1Col && i2Col && i3Col) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            auto* i3 = i3Col->as<ColumnOptVector<String>>();
            if (a && b && c && i1 && i2 && i3) {
                for (size_t idx = 0; idx < a->size(); idx++) {
                    if (a->at(idx) && b->at(idx) && c->at(idx) &&
                        i1->at(idx) && i2->at(idx) && i3->at(idx)) {
                        actual.add({*a->at(idx), *b->at(idx), *c->at(idx),
                                    *i1->at(idx), *i2->at(idx), *i3->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

// Test 58: Cartesian with category chain and person
TEST_F(JoinFeatureTest, commaMatch_categoryChainWithPerson) {
    constexpr std::string_view QUERY = R"(
        MATCH (i:Interest)-->(c:Category), (p:Person)
        WHERE p.isFrench = true AND c.name = 'Cat2'
        RETURN p.name, i.name, c.name
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, String>;

    // Build expected results using reader API
    auto reader = read();
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const LabelID personLabelID = getLabelID("Person");
    const LabelID interestLabelID = getLabelID("Interest");
    const LabelID categoryLabelID = getLabelID("Category");

    // Find French persons
    std::vector<String> frenchPersons;
    for (const auto& nodeID : reader.scanNodes()) {
        NodeView nodeView = reader.getNodeView(nodeID);
        if (nodeView.labelset().hasLabel(personLabelID)) {
            const auto* name = reader.tryGetNodeProperty<types::String>(nameID, nodeID);
            const auto* isFrench = reader.tryGetNodeProperty<types::Bool>(isFrenchID, nodeID);
            if (name && isFrench && *isFrench) {
                frenchPersons.push_back(*name);
            }
        }
    }

    // Find interests that belong to Cat2
    std::vector<String> cat2Interests;
    for (const auto& e : reader.scanOutEdges()) {
        NodeView srcView = reader.getNodeView(e._nodeID);
        NodeView dstView = reader.getNodeView(e._otherID);
        if (srcView.labelset().hasLabel(interestLabelID) &&
            dstView.labelset().hasLabel(categoryLabelID)) {
            const auto* catName = reader.tryGetNodeProperty<types::String>(nameID, e._otherID);
            const auto* iName = reader.tryGetNodeProperty<types::String>(nameID, e._nodeID);
            if (catName && *catName == "Cat2" && iName) {
                cat2Interests.push_back(*iName);
            }
        }
    }

    Rows expected;
    for (const auto& pName : frenchPersons) {
        for (const auto& iName : cat2Interests) {
            expected.add({pName, iName, String("Cat2")});
        }
    }

    Rows actual;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* pCol = findColumn(df, "p.name");
        auto* iCol = findColumn(df, "i.name");
        auto* cCol = findColumn(df, "c.name");
        if (pCol && iCol && cCol) {
            auto* p = pCol->as<ColumnOptVector<String>>();
            auto* i = iCol->as<ColumnOptVector<String>>();
            auto* c = cCol->as<ColumnOptVector<String>>();
            if (p && i && c) {
                for (size_t idx = 0; idx < p->size(); idx++) {
                    if (p->at(idx) && i->at(idx) && c->at(idx)) {
                        actual.add({*p->at(idx), *i->at(idx), *c->at(idx)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    ASSERT_TRUE(expected.equals(actual));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
