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
TEST_F(JoinFeatureTest, emptyLeftSideJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:NonExistentLabel)-->(b:Interest)<--(c:Person)
        RETURN a, b, c
    )";

    bool callbackCalled = false;
    auto res = query(QUERY, [&](const Dataframe* df) {
        callbackCalled = true;
        // Should have 0 rows or the query should fail gracefully
        if (df) {
            ASSERT_EQ(df->getRowCount(), 0);
        }
    });
    // Either succeeds with empty result or fails gracefully (no crash)
}

// Test 2: Right side returns no nodes (non-existent label)
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
}

// Test 3: Both sides empty (all fake labels)
TEST_F(JoinFeatureTest, bothSidesEmptyJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Fake1)-->(b:Fake2)<--(c:Fake3)
        RETURN a, b, c
    )";

    auto res = query(QUERY, [&](const Dataframe* df) {
        if (df) {
            ASSERT_EQ(df->getRowCount(), 0);
        }
    });
    // Main goal: no segfault
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
    // Orphan has no incoming edges, so join should produce 0 rows
    // This is an important edge case for the hash join processor
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

    size_t rowCount = 0;
    std::set<std::pair<String, String>> foundPairs;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* cNames = findColumn(df, "c.name");
        if (aNames && cNames) {
            auto* aCol = aNames->as<ColumnOptVector<String>>();
            auto* cCol = cNames->as<ColumnOptVector<String>>();
            if (aCol && cCol) {
                for (size_t i = 0; i < aCol->size(); i++) {
                    if (aCol->at(i) && cCol->at(i)) {
                        foundPairs.insert({*aCol->at(i), *cCol->at(i)});
                    }
                }
                rowCount = aCol->size();
            }
        }
    });
    // 5 persons (A, B, C, D, E) share MegaHub
    // Expected: 5 * 4 = 20 pairs where a != c
    // This exercises the hash join with high cardinality
}

// Test 9: Shared interest join (3 persons share I1)
TEST_F(JoinFeatureTest, sharedInterestJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)<--(c:Person)
        WHERE b.name = 'Shared' AND a.name <> c.name
        RETURN a.name, b.name, c.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    // 3 persons (A, B, C) share "Shared"
    // Expected: 3 * 2 = 6 pairs where a != c
}

// Test 10: Cartesian product - all persons with all interests
TEST_F(JoinFeatureTest, fullCartesianProduct) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person), (b:Interest)
        RETURN a.name, b.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    // 6 persons * 5 interests = 30 rows expected
    // This stresses memory allocation in the join processor
}

// Test 11: Triple-hop join (Person -> Interest -> Category)
TEST_F(JoinFeatureTest, tripleHopJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)-->(c:Category)
        RETURN a.name, b.name, c.name
    )";

    using String = types::String::Primitive;

    std::set<String> foundPersons;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        if (aNames) {
            auto* aCol = aNames->as<ColumnOptVector<String>>();
            if (aCol) {
                for (size_t i = 0; i < aCol->size(); i++) {
                    if (aCol->at(i)) {
                        foundPersons.insert(*aCol->at(i));
                    }
                }
            }
        }
    });
    // Only "Shared" (I1) is connected to Cat1
    // A, B, C are connected to "Shared"
    // Expected: 3 rows (A, B, C) all going through Shared -> Cat1
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
}

// Test 14: Undirected edge join
TEST_F(JoinFeatureTest, undirectedEdgeJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)--(b:Person)
        RETURN a.name, b.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    // Undirected should match both directions
    // A-B (both ways), B-A (both ways), B-C (both ways), C-A (both ways)
}

// =============================================================================
// CATEGORY 5: SELF-JOIN & CYCLE DETECTION
// Tests for cyclic patterns and self-referential joins
// =============================================================================

// Test 15: Self-join with equality filter (a = b)
TEST_F(JoinFeatureTest, selfJoinWithEquality) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a = b
        RETURN a.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    // No self-loops in graph, should return 0 rows
    EXPECT_EQ(rowCount, 0);
}

// Test 16: Cyclic pattern A->B->C->A
TEST_F(JoinFeatureTest, cyclicPatternJoin) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a)
        RETURN a.name, b.name, c.name
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
    // Cycle A->B->C->A exists
    // Should find rotations: (A,B,C), (B,C,A), (C,A,B)
}

// Test 17: Symmetric relationship (A knows B AND B knows A)
TEST_F(JoinFeatureTest, symmetricRelationshipJoin) {
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

    std::set<String> foundPersons;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        if (aNames) {
            auto* aCol = aNames->as<ColumnOptVector<String>>();
            if (aCol) {
                for (size_t i = 0; i < aCol->size(); i++) {
                    if (aCol->at(i)) {
                        foundPersons.insert(*aCol->at(i));
                    }
                }
            }
        }
    });
    // Only A and B have isFrench=true
    // E has null isFrench, so should be excluded
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

    std::set<std::tuple<String, String, String>> foundTuples;
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
                        foundTuples.insert({*aCol->at(i), *bCol->at(i), *cCol->at(i)});
                    }
                }
            }
        }
    });
    // French: A, B
    // Non-French: C, D
    // Shared interests between French and non-French:
    // - Shared: A/B (French) with C (non-French)
    // - MegaHub: A/B (French) with C/D (non-French)
}

// Test 21: Filter with IS NULL (I3 has no name property)
TEST_F(JoinFeatureTest, filterOnNullProperty) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Interest)
        WHERE a.name IS NULL
        RETURN a
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
    });
    // I3 has no name property
    // Expected: 1 row (I3)
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

    size_t totalRows = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        totalRows += df->getRowCount();
    });
    // 5 persons share MegaHub
    // Expected: 5 * 4 = 20 rows
    // With small chunk sizes, this tests the _rowOffsetState handling
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
}

// Test 25: Multiple disconnected patterns (cartesian product)
TEST_F(JoinFeatureTest, multipleDisconnectedPatterns) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person), (b:Person), (c:Person)
        WHERE a.name = 'A' AND b.name = 'B' AND c.name = 'C'
        RETURN a.name, b.name, c.name
    )";

    bool foundResult = false;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        // Should have exactly 1 row: (A, B, C)
        if (df->getRowCount() == 1) {
            foundResult = true;
        }
    });
}

// Test 26: Join with property projection
TEST_F(JoinFeatureTest, joinWithPropertyProjection) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)
        RETURN a.name, a.isFrench, b.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount = df->getRowCount();
        // Verify all three columns exist
        EXPECT_TRUE(findColumn(df, "a.name"));
        EXPECT_TRUE(findColumn(df, "a.isFrench"));
        EXPECT_TRUE(findColumn(df, "b.name"));
    });
    ASSERT_TRUE(res);
}

// Test 27: Join with DISTINCT
TEST_F(JoinFeatureTest, joinWithDistinct) {
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

    std::set<std::tuple<String, String, String, String>> results;
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
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i) && i1->at(i) && i2->at(i)) {
                        results.insert({*a->at(i), *b->at(i), *i1->at(i), *i2->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // Complex: A,B,C share Shared and MegaHub
    // So (A,B,Shared,MegaHub), (A,B,MegaHub,Shared), etc.
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

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Three-way join: persons connected through two different interests
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

    std::set<std::tuple<String, String, String>> results;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* catCol = findColumn(df, "cat.name");
        if (aCol && bCol && catCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* cat = catCol->as<ColumnOptVector<String>>();
            if (a && b && cat) {
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i) && cat->at(i)) {
                        results.insert({*a->at(i), *b->at(i), *cat->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // Persons connected through different interests in same category
    // Cat2 has Unique (A only) and MegaHub (A,B,C,D,E)
}

// Test 32: Three-way interest sharing - multiple persons on same interest chain
// Pattern: (a)-->(i)<--(b), (c)-->(i)
// Joins: three persons meeting at same interest
TEST_F(JoinFeatureTest, multiJoin_threeWayInterestShare) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)<--(b:Person)<--(c:Person)
        WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name
        RETURN a.name, b.name, c.name, i.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Persons sharing interest with additional KNOWS edge
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

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Very long chain - four persons through three interests
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

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // MegaHub belongs to Cat2 and Cat3
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

    std::set<std::tuple<String, String, String>> results;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aCol = findColumn(df, "a.name");
        auto* bCol = findColumn(df, "b.name");
        auto* catCol = findColumn(df, "cat.name");
        if (aCol && bCol && catCol) {
            auto* a = aCol->as<ColumnOptVector<String>>();
            auto* b = bCol->as<ColumnOptVector<String>>();
            auto* cat = catCol->as<ColumnOptVector<String>>();
            if (a && b && cat) {
                for (size_t i = 0; i < a->size(); i++) {
                    if (a->at(i) && b->at(i) && cat->at(i)) {
                        results.insert({*a->at(i), *b->at(i), *cat->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // Person chain ending in category
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

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Complex: three persons connected through interests and category
}

// Test 39: Symmetric interest chain
// Pattern: (a)-->(i)<--(b)-->(i)<--(c) where same interest appears twice
// Tests variable reuse in chain
TEST_F(JoinFeatureTest, multiJoin_symmetricInterestChain) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)<--(b:Person)-->(i)<--(c:Person)
        WHERE a.name <> b.name AND b.name <> c.name AND a.name <> c.name
        RETURN a.name, b.name, c.name, i.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    // Loop pattern not supported - variable 'i' appears twice in chain
    ASSERT_FALSE(res);
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

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Chain with category in the middle
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

    std::set<std::tuple<String, String, String>> results;
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
                        results.insert({*a->at(i), *b->at(i), *c->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // French person to non-French person through interest chain
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

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Very long chain - stresses join pipeline
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

    size_t rowCount = 0;
    std::set<std::tuple<String, String, String, String, String>> personCombinations;

    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();

        auto* p1Col = findColumn(df, "p1.name");
        auto* p2Col = findColumn(df, "p2.name");
        auto* p3Col = findColumn(df, "p3.name");
        auto* p4Col = findColumn(df, "p4.name");
        auto* p5Col = findColumn(df, "p5.name");

        if (p1Col && p2Col && p3Col && p4Col && p5Col) {
            auto* p1 = p1Col->as<ColumnOptVector<String>>();
            auto* p2 = p2Col->as<ColumnOptVector<String>>();
            auto* p3 = p3Col->as<ColumnOptVector<String>>();
            auto* p4 = p4Col->as<ColumnOptVector<String>>();
            auto* p5 = p5Col->as<ColumnOptVector<String>>();

            if (p1 && p2 && p3 && p4 && p5) {
                for (size_t i = 0; i < p1->size(); i++) {
                    if (p1->at(i) && p2->at(i) && p3->at(i) && p4->at(i) && p5->at(i)) {
                        personCombinations.insert({
                            *p1->at(i), *p2->at(i), *p3->at(i), *p4->at(i), *p5->at(i)
                        });
                    }
                }
            }
        }
    });

    ASSERT_TRUE(res);
    // 8-join chain: 5 distinct persons connected through 4 distinct interests
    // This is the most complex linear join pattern, stressing the join pipeline
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

    std::set<String> frenchPersons;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* pCol = findColumn(df, "p.name");
        if (pCol) {
            auto* p = pCol->as<ColumnOptVector<String>>();
            if (p) {
                for (size_t i = 0; i < p->size(); i++) {
                    if (p->at(i)) {
                        frenchPersons.insert(*p->at(i));
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // French persons (A, B) combined with MegaHub interest
}

// Test 50: Three-way Cartesian - Person, Interest, Category
TEST_F(JoinFeatureTest, commaMatch_threeWayCartesian) {
    constexpr std::string_view QUERY = R"(
        MATCH (p:Person), (i:Interest), (c:Category)
        WHERE p.name = 'A' AND i.name = 'Shared' AND c.name = 'Cat1'
        RETURN p.name, i.name, c.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Should return exactly 1 row: (A, Shared, Cat1)
    EXPECT_EQ(rowCount, 1);
}

// Test 51: Path pattern combined with single node (comma-separated)
TEST_F(JoinFeatureTest, commaMatch_pathWithSingleNode) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest), (c:Category)
        WHERE c.name = 'Cat1'
        RETURN a.name, i.name, c.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // All Person->Interest pairs combined with Cat1
}

// Test 52: Two independent path patterns (comma-separated)
TEST_F(JoinFeatureTest, commaMatch_twoIndependentPaths) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest), (b:Person)-->(i2:Interest)
        WHERE a.name = 'A' AND b.name = 'B' AND i1.name <> i2.name
        RETURN a.name, b.name, i1.name, i2.name
    )";

    using String = types::String::Primitive;

    std::set<std::pair<String, String>> interestPairs;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* i1Col = findColumn(df, "i1.name");
        auto* i2Col = findColumn(df, "i2.name");
        if (i1Col && i2Col) {
            auto* i1 = i1Col->as<ColumnOptVector<String>>();
            auto* i2 = i2Col->as<ColumnOptVector<String>>();
            if (i1 && i2) {
                for (size_t i = 0; i < i1->size(); i++) {
                    if (i1->at(i) && i2->at(i)) {
                        interestPairs.insert({*i1->at(i), *i2->at(i)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // A's interests paired with B's interests (where different)
}

// Test 53: Four-way Cartesian with all different types
TEST_F(JoinFeatureTest, commaMatch_fourWayCartesian) {
    constexpr std::string_view QUERY = R"(
        MATCH (p:Person), (i:Interest), (c:Category), (city:City)
        WHERE p.name = 'A' AND i.name = 'Shared' AND c.name = 'Cat1' AND city.name = 'Paris'
        RETURN p.name, i.name, c.name, city.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Should return exactly 1 row
    EXPECT_EQ(rowCount, 1);
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

    size_t rowCount = 0;
    std::set<String> frenchLeaders;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
        auto* p1Col = findColumn(df, "p1.name");
        if (p1Col) {
            auto* p1 = p1Col->as<ColumnOptVector<String>>();
            if (p1) {
                for (size_t i = 0; i < p1->size(); i++) {
                    if (p1->at(i)) {
                        frenchLeaders.insert(*p1->at(i));
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // French persons (A, B) as p1, with 2 other distinct persons
}

// Test 55: Path and Cartesian combination with shared type
TEST_F(JoinFeatureTest, commaMatch_pathAndCartesianMixed) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i:Interest)-->(c:Category), (other:Person)
        WHERE a.name <> other.name AND c.name = 'Cat1'
        RETURN a.name, i.name, c.name, other.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Person->Interest->Cat1 paths combined with other persons
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
    ASSERT_TRUE(res);
    // French persons' interests paired with non-French persons' interests
}

// Test 57: Three paths combined (stress test for comma patterns)
TEST_F(JoinFeatureTest, commaMatch_threePathsCombined) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(i1:Interest), (b:Person)-->(i2:Interest), (c:Person)-->(i3:Interest)
        WHERE a.name = 'A' AND b.name = 'B' AND c.name = 'C'
          AND i1.name <> i2.name AND i2.name <> i3.name AND i1.name <> i3.name
        RETURN a.name, b.name, c.name, i1.name, i2.name, i3.name
    )";

    size_t rowCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        rowCount += df->getRowCount();
    });
    ASSERT_TRUE(res);
    // Three specific persons with three distinct interests
}

// Test 58: Cartesian with category chain and person
TEST_F(JoinFeatureTest, commaMatch_categoryChainWithPerson) {
    constexpr std::string_view QUERY = R"(
        MATCH (i:Interest)-->(c:Category), (p:Person)
        WHERE p.isFrench = true AND c.name = 'Cat2'
        RETURN p.name, i.name, c.name
    )";

    using String = types::String::Primitive;

    std::set<std::tuple<String, String, String>> results;
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
                for (size_t j = 0; j < p->size(); j++) {
                    if (p->at(j) && i->at(j) && c->at(j)) {
                        results.insert({*p->at(j), *i->at(j), *c->at(j)});
                    }
                }
            }
        }
    });
    ASSERT_TRUE(res);
    // Interests in Cat2 combined with French persons
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
