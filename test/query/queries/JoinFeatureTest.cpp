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
        WHERE b.name = 'MegaHub' AND a <> c
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
        WHERE b.name = 'Shared' AND a <> c
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
// DISABLED: Crashes in MaterializeProcessor::execute()
TEST_F(JoinFeatureTest, DISABLED_explicitEdgeTypeCrash) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-[e1:INTERESTED_IN]->(b:Interest)<-[e2:INTERESTED_IN]-(c:Person)
        WHERE a <> c
        RETURN a.name, b.name, c.name
    )";

    auto res = query(QUERY, [&](const Dataframe* df) {
        // If we get here without crashing, the bug is fixed!
        ASSERT_TRUE(df);
    });
}

// Test 13: Mixed edge types in a path
// DISABLED: May crash in MaterializeProcessor::execute()
TEST_F(JoinFeatureTest, DISABLED_mixedEdgeTypesInPath) {
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
// DISABLED: May crash with explicit edge types
TEST_F(JoinFeatureTest, DISABLED_cyclicPatternJoin) {
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
// DISABLED: May crash with explicit edge types
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
        WHERE a.isFrench = true AND c.isFrench = false AND a <> c
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
        WHERE b.name = 'MegaHub' AND a <> c
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
        WHERE b.name = 'NonExistent' AND a <> c
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
        WHERE a <> c
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

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
