#include <gtest/gtest.h>

#include "SystemManager.h"
#include "TuringDB.h"
#include "QueryConfig.h"
#include "QueryStatus.h"
#include "SimpleGraph.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "GraphQueryTest.h"

using namespace turing::test;

class CallProcedureTest : public GraphQueryTest {
protected:
    // Returns the node count for a particular label in the output of the hirearchical count
    // procedure
    size_t hierarchicalCount(std::string_view listArg, std::string_view wanted) {
        size_t found = 0;
        const std::string q = std::string("CALL db.hierarchicalLabelCounts([") +
                              std::string(listArg) +
                              "]) YIELD label, nodeCount RETURN label, nodeCount";
        const auto res = query(q, [&](const Dataframe* df) -> void {
            const auto* labels = df->cols().at(0)->as<ColumnVector<std::string_view>>();
            const auto* counts = df->cols().at(1)->as<ColumnVector<uint64_t>>();
            if (labels == nullptr || counts == nullptr) {
                return;
            }
            for (size_t i = 0; i < labels->size(); ++i) {
                if (labels->at(i) == wanted) {
                    found = static_cast<int64_t>(counts->at(i));
                    break;
                }
            }
        });
        EXPECT_TRUE(res.isOk());
        return found;
    }

    // Total number of result rows a query yields across all chunks.
    size_t rowCount(const std::string& q) {
        size_t n = 0;
        const auto res = query(q, [&](const Dataframe* df) {
            if (df) {
                n += df->getLogicalRowCount();
            }
        });
        EXPECT_TRUE(res.isOk());
        return n;
    }
};

TEST_F(CallProcedureTest, Labels) {
    bool executed = false;
    const auto res = query("CALL db.labels()", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 2);
        ASSERT_EQ(df->getLogicalRowCount(), 9);

        executed = true;
    });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, EdgeTypes) {
    bool executed = false;
    const auto res = query("CALL db.edgeTypes()", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 2);
        ASSERT_EQ(df->getLogicalRowCount(), 2);

        executed = true;
    });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, PropertyTypes) {
    bool executed = false;
    const auto res = query("CALL db.propertyTypes()", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 3);
        ASSERT_EQ(df->getLogicalRowCount(), 8);

        executed = true;
    });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, History) {
    bool executed = false;
    const auto res = query("CALL db.propertyTypes()", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 3);
        ASSERT_EQ(df->getLogicalRowCount(), 8);

        executed = true;
    });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, NeighbourhoodSampleOverAMatchWithoutARow) {
    // Nobody is named "nobody", so the call is driven on an empty argument column. An
    // empty chunk is a normal chunk: the query returns no row rather than failing.
    const size_t rows = rowCount("MATCH (n:Person) WHERE n.name = \"nobody\" "
                                 "CALL gnn.neighbourhoodSample(n, 5, 1) YIELD tgt "
                                 "RETURN n, tgt");
    EXPECT_EQ(rows, 0);
}

TEST_F(CallProcedureTest, DescribeCommit) {
    bool executed = false;
    const auto res = query("CALL db.history() YIELD commit AS c "
                           "CALL db.describeCommit(c) YIELD nodeCount, edgeCount "
                           "RETURN nodeCount, edgeCount",
                           [&](const Dataframe* df) -> void {
                               ASSERT_TRUE(df != nullptr);
                               ASSERT_EQ(df->cols().size(), 2);
                               ASSERT_EQ(df->getLogicalRowCount(), 8);

                               const auto& cols = df->cols();

                               const auto* nodeCounts = cols.at(0)->as<ColumnVector<uint64_t>>();
                               const auto* edgeCounts = cols.at(1)->as<ColumnVector<uint64_t>>();

                               ASSERT_TRUE(nodeCounts != nullptr);
                               ASSERT_TRUE(edgeCounts != nullptr);

                               const auto check = [&](size_t i, uint64_t nodeCount, uint64_t edgeCount) {
                                   EXPECT_EQ(nodeCounts->at(i), nodeCount);
                                   EXPECT_EQ(edgeCounts->at(i), edgeCount);
                               };

                               check(0, 2, 3);
                               check(1, 3, 2);
                               check(2, 1, 0);
                               check(3, 1, 1);
                               check(4, 2, 2);
                               check(5, 2, 2);
                               check(6, 7, 8);
                               check(7, 0, 0);

                               executed = true;
                           });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, YieldWhereSelfJoin) {
    // Self-join on label names: CALL db.labels() twice with WHERE a = b
    // Should return 9 (one per label), not 81 (9 * 9 cartesian)
    bool executed = false;
    const auto res = query("CALL db.labels() YIELD label AS a "
                           "CALL db.labels() YIELD label AS b "
                           "WHERE a = b RETURN count(*)",
                           [&](const Dataframe* df) -> void {
                               ASSERT_TRUE(df != nullptr);
                               ASSERT_EQ(df->cols().size(), 1);

                               const auto* col = df->cols().at(0)->as<ColumnConst<uint64_t>>();
                               ASSERT_TRUE(col != nullptr);
                               ASSERT_EQ(col->at(0), 9u);

                               executed = true;
                           });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, YieldWhereCrossJoin) {
    // Cross-join between labels and propertyTypes on their string columns
    // Labels: 9 entries (label), PropertyTypes: 8 entries (propertyType)
    // WHERE a = b should only match where a label name equals a property type name
    bool executed = false;
    const auto res = query("CALL db.labels() YIELD label AS a "
                           "CALL db.propertyTypes() YIELD propertyType AS b "
                           "WHERE a = b RETURN count(*)",
                           [&](const Dataframe* df) -> void {
                               ASSERT_TRUE(df != nullptr);
                               ASSERT_EQ(df->cols().size(), 1);

                               const auto* col = df->cols().at(0)->as<ColumnConst<uint64_t>>();
                               ASSERT_TRUE(col != nullptr);
                               // Result must be less than 9 * 8 = 72 (cartesian product)
                               EXPECT_LT(col->at(0), 72u);

                               executed = true;
                           });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, YieldWhereSelfJoinByID) {
    // Self-join on label IDs: both sides are LabelID, so hash join works
    // Should return 9 (one per label), not 81 (9 * 9 cartesian)
    bool executed = false;
    const auto res = query("CALL db.labels() YIELD id AS l "
                           "CALL db.labels() YIELD id AS m "
                           "WHERE l = m RETURN count(*)",
                           [&](const Dataframe* df) -> void {
                               ASSERT_TRUE(df != nullptr);
                               ASSERT_EQ(df->cols().size(), 1);

                               const auto* col = df->cols().at(0)->as<ColumnConst<uint64_t>>();
                               ASSERT_TRUE(col != nullptr);
                               ASSERT_EQ(col->at(0), 9u);

                               executed = true;
                           });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, YieldWhereCrossJoinIncompatibleTypes) {
    // Exact query from issue #549: LabelID != PropertyTypeID
    const auto res = query("CALL db.labels() YIELD id AS l "
                           "CALL db.propertyTypes() YIELD id AS p "
                           "WHERE l = p RETURN count(*)",
                           [&](const Dataframe*) -> void {});

    EXPECT_EQ(res.getStatus(), QueryStatus::Status::ANALYZE_ERROR);
}

TEST_F(CallProcedureTest, HierarchicalLabelCountsExcludesDeletedNodes) {
    // Baseline: 8 Person nodes (spread across parts); Doruk is the only Sales node.
    EXPECT_EQ(hierarchicalCount("", "Person"), 8);
    EXPECT_EQ(hierarchicalCount("", "Sales"), 1);
    EXPECT_EQ(hierarchicalCount("'Person'", "Sales"), 1);

    // Delete Doruk (Person + Sales).
    newChange();
    {
        const auto res = query("MATCH (n {name: 'Doruk'}) DELETE n",
                               [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
    }
    submitCurrentChange();

    // The tombstoned node must no longer be counted.
    EXPECT_EQ(hierarchicalCount("", "Person"), 7); // 8 -> 7
    // Sales had only Doruk -> count 0 -> dropped from the results entirely.
    EXPECT_EQ(hierarchicalCount("", "Sales"), 0);
    EXPECT_EQ(hierarchicalCount("'Person'", "Sales"), 0);
}

// db.listNodes must exclude tombstoned (deleted) nodes. The base scan iterators
// it uses (scanNodes / scanNodesByLabel / scanNodeProperties) don't filter
// tombstones themselves, so the procedure checks each candidate against the
// graph's tombstone set. This lives in C++ (not the JSON query-test-suite)
// because it needs a delete + submit before the assertion, mirroring
// HierarchicalLabelCountsExcludesDeletedNodes.
TEST_F(CallProcedureTest, ListNodesExcludesDeletedNodes) {
    // count(*) can't distinguish "0 rows" (global aggregate over an empty input
    // yields 1), so count rows directly for the "should be empty" case.
    const auto scalarCount = [&](const std::string& q) -> uint64_t {
        uint64_t c = 0;
        const auto res = query(q, [&](const Dataframe* df) {
            if (!df) {
                return;
            }
            const auto* col = df->cols().at(0)->as<ColumnConst<uint64_t>>();
            if (col) {
                c = col->at(0);
            }
        });
        EXPECT_TRUE(res.isOk());
        return c;
    };

    // Doruk is a Person + Sales node named "Doruk".
    const std::string ALL =
        "CALL db.listNodes([], [], [], 0, 1000) YIELD id, labels, properties RETURN count(*)";
    const std::string PERSON =
        "CALL db.listNodes(['Person'], [], [], 0, 1000) YIELD id, labels, properties RETURN count(*)";
    const std::string BY_NAME =
        "CALL db.listNodes([], ['name'], ['doruk'], 0, 1000) YIELD id, labels, properties RETURN id";

    EXPECT_EQ(scalarCount(ALL), 18u);
    EXPECT_EQ(scalarCount(PERSON), 8u);
    EXPECT_EQ(rowCount(BY_NAME), 1u);

    newChange();
    {
        const auto res = query("MATCH (n {name: 'Doruk'}) DELETE n", [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
    }
    submitCurrentChange();

    // The tombstoned node must drop out of every scan path: unfiltered, label-
    // filtered, and property-filtered.
    EXPECT_EQ(scalarCount(ALL), 17u);
    EXPECT_EQ(scalarCount(PERSON), 7u);
    EXPECT_EQ(rowCount(BY_NAME), 0u);
}

// db.getEdges must skip deleted (tombstoned) edges. Lives in C++ (not the JSON
// suite) because it needs a delete + submit before the assertion.
TEST_F(CallProcedureTest, GetEdgesExcludesDeletedEdges) {
    // Edges 0..2 are the first three created in SimpleGraph (Remy->Adam,
    // Adam->Remy, Remy->Ghosts).
    const std::string GET =
        "CALL db.getEdges([0, 1, 2]) YIELD id, src, tgt, edgeTypeID, properties RETURN id";
    EXPECT_EQ(rowCount(GET), 3u);

    newChange();
    {
        const auto res = query("MATCH (a {name: 'Remy'})-[e]->(b {name: 'Adam'}) DELETE e",
                               [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
    }
    submitCurrentChange();

    // The deleted Remy->Adam edge drops out; the other two remain.
    EXPECT_EQ(rowCount(GET), 2u);
}

// db.getNodes must skip deleted (tombstoned) nodes. Lives in C++ (not the JSON
// suite) because it needs a delete + submit before the assertion.
TEST_F(CallProcedureTest, GetNodesExcludesDeletedNodes) {
    // SimpleGraph has 18 nodes with ids 0..17.
    const std::string GET =
        "CALL db.getNodes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]) "
        "YIELD id, labels, inEdgeCount, outEdgeCount, properties RETURN id";
    EXPECT_EQ(rowCount(GET), 18u);

    newChange();
    {
        const auto res = query("MATCH (n {name: 'Doruk'}) DELETE n", [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
    }
    submitCurrentChange();

    // The tombstoned node drops out; the rest are still fetched by id.
    EXPECT_EQ(rowCount(GET), 17u);
}

// db.getNodeEdges must skip deleted (tombstoned) nodes (the queried node gets no
// row). Lives in C++ because it needs a delete + submit before the assertion.
TEST_F(CallProcedureTest, GetNodeEdgesExcludesDeletedNodes) {
    // SimpleGraph has 18 nodes with ids 0..17.
    const std::string GET =
        "CALL db.getNodeEdges([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17], "
        "10, [], [], [], [], false) "
        "YIELD id, outgoingEdges, incomingEdges, outEdgeCounts, inEdgeCounts RETURN id";
    EXPECT_EQ(rowCount(GET), 18u);

    newChange();
    {
        const auto res = query("MATCH (n {name: 'Doruk'}) DELETE n", [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
    }
    submitCurrentChange();

    EXPECT_EQ(rowCount(GET), 17u);
}

// db.getNodeEdges must skip deleted (tombstoned) edges: a deleted edge must not
// appear in the outgoing/incoming lists nor be tallied in the counts. The base
// getOutEdges/getInEdges iterators don't filter tombstones, so the procedure
// checks each edge. Lives in C++ because it needs a delete + submit.
TEST_F(CallProcedureTest, GetNodeEdgesExcludesDeletedEdges) {
    // Size of the outgoingEdges list (row 0) for node 0 (Remy).
    const auto outgoingEdgeCount = [&](const std::string& q) -> size_t {
        size_t n = 0;
        const auto res = query(q, [&](const Dataframe* df) {
            if (!df) {
                return;
            }
            const auto* outs = df->cols().at(1)->as<ColumnVector<ListView>>();
            if (outs && outs->size() > 0) {
                n = outs->at(0).size();
            }
        });
        EXPECT_TRUE(res.isOk());
        return n;
    };

    // Remy (node 0) has 4 outgoing edges (ids 0..3). High limit -> no truncation.
    const std::string GET =
        "CALL db.getNodeEdges([0], 1000, [], [], [], [], false) "
        "YIELD id, outgoingEdges, incomingEdges, outEdgeCounts, inEdgeCounts "
        "RETURN id, outgoingEdges";
    EXPECT_EQ(outgoingEdgeCount(GET), 4u);

    newChange();
    {
        const auto res = query("MATCH (a {name: 'Remy'})-[e]->(b {name: 'Adam'}) DELETE e",
                               [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
    }
    submitCurrentChange();

    // The deleted Remy->Adam edge must drop out of the outgoing list.
    EXPECT_EQ(outgoingEdgeCount(GET), 3u);
}

// A non-finite Double property (nan/inf) must serialize as JSON `null`, since
// nan/inf aren't valid JSON tokens and would break the visualiser's JSON.parse.
// Needs a write (to store the value), so it lives in C++.
// A non-finite Double property must serialize as JSON `null` — nan/inf aren't
// valid JSON and would break the visualiser's JSON.parse. The query layer blocks
// 0.0/0.0, but multiplication overflow (1e308*1e308) yields +inf, which stores as
// a Double and exercises the same isfinite path. Needs a write, so it's in C++.
TEST_F(CallProcedureTest, NonFiniteDoublePropertySerializesAsNull) {
    newChange();
    {
        const auto res = query("CREATE (n:InfTest {score: 1e308 * 1e308})", [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
    }
    submitCurrentChange();

    std::string props;
    const auto res = query(
        "CALL db.listNodes(['InfTest'], [], [], 0, 10) YIELD id, labels, properties "
        "RETURN properties",
        [&](const Dataframe* df) {
            if (!df) {
                return;
            }
            const auto* col = df->cols().at(0)->as<ColumnVector<std::string>>();
            if (col && col->size() > 0) {
                props = col->at(0);
            }
        });
    EXPECT_TRUE(res.isOk());
    // The non-finite value is emitted as null, never as inf/nan.
    EXPECT_NE(props.find("\"score\":null"), std::string::npos) << "props=" << props;
    EXPECT_EQ(props.find("inf"), std::string::npos) << "props=" << props;
    EXPECT_EQ(props.find("nan"), std::string::npos) << "props=" << props;
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
