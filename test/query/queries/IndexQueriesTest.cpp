#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <vector>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/Transaction.h"
#include "dataframe/Dataframe.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "ID.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class IndexQueriesTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;
    ChangeID _currentChange {ChangeID::head()};

    static constexpr auto emptyCallback = [](const Dataframe*) -> void {};

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);

        Change* change = res.value();
        _currentChange = change->id();
    }

    auto query(std::string_view q, auto callback, ChangeID change) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), change);
        return _db->query(q, state);
    }

    auto query(std::string_view q, auto callback) {
        return query(q, callback, _currentChange);
    }

    void submitCurrentChange() {
        auto res = query("CHANGE SUBMIT", emptyCallback);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void submitChange(ChangeID chid) {
        auto res = query("CHANGE SUBMIT", emptyCallback, chid);
        ASSERT_TRUE(res) << res.getError();
        _currentChange = ChangeID::head();
    }

    void setChange(ChangeID chid) {
        _currentChange = chid;
    }

    // Counts the total number of indexes visible at the current head.
    size_t countIndexes() {
        size_t count = 0;
        auto res = query("CALL db.showIndexes() YIELD name", [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            count += df->getLogicalRowCount();
        });
        EXPECT_TRUE(res) << res.getError();
        return count;
    }

    // Collect all NodeIDs from the named column across all result chunks.
    std::vector<NodeID> collectNodes(std::string_view queryStr, std::string_view colName) {
        std::vector<NodeID> results;
        auto res = query(queryStr, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* col = findColumn(df, colName);
            ASSERT_TRUE(col) << "Column '" << colName << "' not found";
            auto* nodes = col->as<ColumnNodeIDs>();
            ASSERT_TRUE(nodes);
            for (NodeID id : *nodes) {
                results.push_back(id);
            }
        });
        EXPECT_TRUE(res) << res.getError();
        return results;
    }

    // Returns true if an index with the given name is visible at the current head.
    bool hasIndex(std::string_view indexName) {
        bool found = false;
        auto res = query("CALL db.showIndexes() YIELD name", [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* col = findColumn(df, "name");
            ASSERT_TRUE(col);
            auto* vec = col->as<ColumnVector<std::string_view>>();
            ASSERT_TRUE(vec);
            for (auto name : *vec) {
                if (name == indexName) {
                    found = true;
                }
            }
        });
        EXPECT_TRUE(res) << res.getError();
        return found;
    }
};

TEST_F(IndexQueriesTest, noIndexesInitially) {
    EXPECT_EQ(0, countIndexes());
}

TEST_F(IndexQueriesTest, indexHasCorrectName) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX myindex FOR (n) ON n.age", emptyCallback));
    EXPECT_EQ(0, countIndexes());

    ASSERT_TRUE(query("commit", emptyCallback));

    const auto findIndex = [&](const Dataframe* df) -> bool {
        EXPECT_TRUE(df);

        auto* names = findColumn(df, "name");
        EXPECT_TRUE(names) << "Expected 'name' column in showIndexes output";

        auto* nameVec = names->as<ColumnVector<std::string_view>>();
        EXPECT_TRUE(nameVec);

        EXPECT_EQ(1, nameVec->size());

        return nameVec->front() == "myindex";
    };

    EXPECT_EQ(1, countIndexes());

    {
        const auto res = query("CALL db.showIndexes() YIELD name, size",
                               [&](const Dataframe* df) { ASSERT_TRUE(findIndex(df)); });

        ASSERT_TRUE(res) << res.getError();
    }

    submitCurrentChange();
    {
        const auto res = query("CALL db.showIndexes() YIELD name, size",
                               [&](const Dataframe* df) { ASSERT_TRUE(findIndex(df)); });

        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(IndexQueriesTest, indexSizeMatchesNodeCount) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX ageindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    bool found = false;
    auto res = query("CALL db.showIndexes() YIELD name, size", [&](const Dataframe* df) {
        ASSERT_TRUE(df);

        auto* names = findColumn(df, "name");
        auto* sizes = findColumn(df, "size");
        ASSERT_TRUE(names) << "Expected 'name' column in showIndexes output";
        ASSERT_TRUE(sizes) << "Expected 'size' column in showIndexes output";

        auto* nameVec = names->as<ColumnVector<std::string_view>>();
        auto* sizeVec = sizes->as<ColumnVector<types::UInt64::Primitive>>();
        ASSERT_TRUE(nameVec);
        ASSERT_TRUE(sizeVec);

        for (size_t i = 0; i < nameVec->size(); i++) {
            if (nameVec->at(i) == "ageindex") {
                found = true;
                EXPECT_EQ(2, sizeVec->at(i))
                    << "Expected index size 2 (Remy and Adam have 'age'), got "
                    << sizeVec->at(i);
            }
        }
    });
    ASSERT_TRUE(res) << res.getError();
    EXPECT_TRUE(found) << "Index 'ageindex' was not found in db.showIndexes()";
}

TEST_F(IndexQueriesTest, indexPersistsAfterSubsequentCommit) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX persistindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    // Second change: add a node; does not affect the index
    newChange();
    ASSERT_TRUE(query("CREATE (n:TestNode { val: 1 })", emptyCallback));
    submitCurrentChange();

    bool found = false;
    auto res = query("CALL db.showIndexes() YIELD name", [&](const Dataframe* df) {
        ASSERT_TRUE(df);

        auto* names = findColumn(df, "name");
        ASSERT_TRUE(names);

        auto* nameVec = names->as<ColumnVector<std::string_view>>();
        ASSERT_TRUE(nameVec);

        for (auto i : *nameVec) {
            if (i == "persistindex") {
                found = true;
            }
        }
    });
    ASSERT_TRUE(res) << res.getError();
    EXPECT_TRUE(found) << "Index 'persistindex' should still be visible after a subsequent commit";
}

TEST_F(IndexQueriesTest, indexSurvivesWriteBufferRebase) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));

    newChange(), change2 = _currentChange;
    ASSERT_TRUE(query("CREATE (n:RebaseTestNode { val: 99 })", emptyCallback));

    // Submit change2 first so that change1 must rebase when submitted
    submitChange(change2);
    submitChange(change1);

    EXPECT_EQ(1, countIndexes());
}

TEST_F(IndexQueriesTest, validIndexSurvivesRebase) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    ASSERT_TRUE(query("CREATE (n:RebaseTestNode { val: 99 })", emptyCallback));

    // Submit change2 first so that change1 must rebase when submitted
    submitChange(change2);
    submitChange(change1);

    EXPECT_EQ(1, countIndexes());
}

TEST_F(IndexQueriesTest, indexDroppedAfterPropSETConflict) {
    newChange();

    {
        ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));
        ASSERT_TRUE(query("commit", emptyCallback));
        ASSERT_EQ(1, countIndexes());

        // Update the age property, so invalidate it
        ASSERT_TRUE(
            query(R"(MATCH (n) WHERE n.name = "Cyrus" SET n.age = 32)", emptyCallback));
        ASSERT_TRUE(query("commit", emptyCallback));
        ASSERT_EQ(0, countIndexes());
    }
}

TEST_F(IndexQueriesTest, indexDroppedAfterPropSETConflictSubmit) {
    newChange();

    {
        ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));
        ASSERT_TRUE(query("commit", emptyCallback));
        ASSERT_EQ(1, countIndexes());

        // Update the age property, so invalidate it
        ASSERT_TRUE(
            query(R"(MATCH (n) WHERE n.name = "Cyrus" SET n.age = 32)", emptyCallback));
        submitCurrentChange();
        ASSERT_EQ(0, countIndexes());
    }
}

TEST_F(IndexQueriesTest, indexDroppedAfterPropCREATEConflict) {
    newChange();

    {
        ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));
        ASSERT_TRUE(query("commit", emptyCallback));
        ASSERT_EQ(1, countIndexes());

        // Update the age property, so invalidate it
        ASSERT_TRUE(
            query(R"(CREATE (n:Person{age:100}))", emptyCallback));
        ASSERT_TRUE(query("commit", emptyCallback));
        ASSERT_EQ(0, countIndexes());
    }
}

TEST_F(IndexQueriesTest, indexDroppedAfterPropCREATEConflictSubmit) {
    newChange();

    {
        ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));
        ASSERT_TRUE(query("commit", emptyCallback));
        ASSERT_EQ(1, countIndexes());

        // Update the age property, so invalidate it
        ASSERT_TRUE(
            query(R"(CREATE (n:Person{age:100}))", emptyCallback));
        submitCurrentChange();
        ASSERT_EQ(0, countIndexes());
    }
}

// A committed edge index survives rebase when no intervening commit writes the indexed property
TEST_F(IndexQueriesTest, edgeIndexSurvivesRebase) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX durationidx FOR [e] ON e.duration", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    // Creates a node with a new property — does not update the property indexed
    ASSERT_TRUE(query("CREATE (n:TestNode { val: 99 })", emptyCallback));

    submitChange(change2);
    submitChange(change1);

    EXPECT_TRUE(hasIndex("durationidx"));
}

// A committed edge index is dropped after rebase when an intervening commit writes
// the same edge property
TEST_F(IndexQueriesTest, edgeIndexDroppedAfterEdgePropConflict) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX durationidx FOR [e] ON e.duration", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    // Write to the exact edge property the index covers.
    ASSERT_TRUE(query("MATCH ()-[e:KNOWS_WELL]->() SET e.duration = 25", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    // Submit change2 first so change1 must rebase. The rebaser sees that 'duration'
    // was written to an edge by change2, so durationidx must be invalidated.
    submitChange(change2);
    submitChange(change1);

    EXPECT_FALSE(hasIndex("durationidx"));
}

// addValidIndexes uses Index::isNodeIndex() to separate node and edge write sets.
// Writing an edge property must NOT invalidate a node index that covers a property
// of the same name (same PropertyTypeID, but different entity kind).
TEST_F(IndexQueriesTest, nodeIndexNotInvalidatedByEdgePropWrite) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    // 'name' exists on both nodes and edges in the simple graph.
    ASSERT_TRUE(query("CREATE INDEX nameidx FOR (n) ON n.name", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    // Write to the edge 'name' property — same PropertyTypeID as node 'name',
    // but this goes into edgePropertyWriteSet, not nodePropertyWriteSet.
    ASSERT_TRUE(query("MATCH ()-[e:KNOWS_WELL]->() SET e.name = \"Updated\"", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    submitChange(change2);
    submitChange(change1);

    EXPECT_TRUE(hasIndex("nameidx"));
}

// The symmetric case: writing a node property must NOT invalidate an edge index
// that covers a property of the same name.
TEST_F(IndexQueriesTest, edgeIndexNotInvalidatedByNodePropWrite) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX nameidx FOR [e] ON e.name", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    // Write to the node 'name' property — goes into nodePropertyWriteSet only.
    ASSERT_TRUE(query(R"(MATCH (n) WHERE n.name = "Remy" SET n.name = "Remy2")", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    submitChange(change2);
    submitChange(change1);

    EXPECT_TRUE(hasIndex("nameidx"));
}

// When a change is rebased, addValidIndexes also pulls in any indexes that live on
// the new head. Verify this merging works across node and edge indexes.
TEST_F(IndexQueriesTest, headIndexesMergedAfterRebase) {
    ChangeID change1, change2;

    // change1 creates an edge index and commits it.
    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX durationidx FOR [e] ON e.duration", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    // change2 creates a node index and commits it.
    newChange(), change2 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX ageidx FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    // Submit change1 first — it becomes the new head with durationidx.
    // Submit change2 second — it must rebase. addValidIndexes should:
    //   - keep ageidx (no conflict with change1)
    //   - pull in durationidx from the new head
    submitChange(change1);
    submitChange(change2);

    EXPECT_TRUE(hasIndex("ageidx"));
    EXPECT_TRUE(hasIndex("durationidx"));
}

// A change that commits both a node index and an edge index should have both survive
// rebase against an unrelated change.
TEST_F(IndexQueriesTest, bothNodeAndEdgeIndexesSurviveUnrelatedRebase) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX ageidx FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_TRUE(query("CREATE INDEX durationidx FOR [e] ON e.duration", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    // Writes only a fresh property not covered by either index.
    ASSERT_TRUE(query("CREATE (n:TestNode { val: 1 })", emptyCallback));

    submitChange(change2);
    submitChange(change1);

    EXPECT_TRUE(hasIndex("ageidx"));
    EXPECT_TRUE(hasIndex("durationidx"));
}

TEST_F(IndexQueriesTest, ageIndexUsedInTraversal) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX ageindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    ASSERT_TRUE(hasIndex("ageindex"));

    const std::vector<NodeID> results =
        collectNodes("MATCH (n)-->(m) WHERE n.age = 32 RETURN m", "m");

    ASSERT_EQ(7u, results.size());

    // Spot-check a sample of the expected neighbours.
    const NodeID adamID   = SimpleGraph::findNodeID(_graph, "Adam");
    const NodeID ghostsID = SimpleGraph::findNodeID(_graph, "Ghosts");
    const NodeID remyID   = SimpleGraph::findNodeID(_graph, "Remy");
    const NodeID bioID    = SimpleGraph::findNodeID(_graph, "Bio");

    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), adamID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), ghostsID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), remyID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), bioID));
}

TEST_F(IndexQueriesTest, ageIndexUsedInTwoHopTraversal) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX ageindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    ASSERT_TRUE(hasIndex("ageindex"));

    const std::vector<NodeID> results =
        collectNodes("MATCH (n)-->(m)-->(p) WHERE n.age = 32 RETURN p", "p");

    ASSERT_EQ(8u, results.size());

    const NodeID remyID      = SimpleGraph::findNodeID(_graph, "Remy");
    const NodeID adamID      = SimpleGraph::findNodeID(_graph, "Adam");
    const NodeID ghostsID    = SimpleGraph::findNodeID(_graph, "Ghosts");
    const NodeID computersID = SimpleGraph::findNodeID(_graph, "Computers");
    const NodeID bioID       = SimpleGraph::findNodeID(_graph, "Bio");
    const NodeID cookingID   = SimpleGraph::findNodeID(_graph, "Cooking");

    // Remy and Adam both appear more than once.
    EXPECT_GE(std::count(results.begin(), results.end(), remyID), 1);
    EXPECT_GE(std::count(results.begin(), results.end(), adamID), 1);
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), ghostsID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), computersID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), bioID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), cookingID));
}

TEST_F(IndexQueriesTest, ageIndexUsedInBidirectionalTraversal) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX ageindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    ASSERT_TRUE(hasIndex("ageindex"));

    const std::vector<NodeID> results =
        collectNodes("MATCH (n)--(m) WHERE n.age = 32 RETURN m", "m");

    ASSERT_EQ(10u, results.size());

    const NodeID remyID      = SimpleGraph::findNodeID(_graph, "Remy");
    const NodeID adamID      = SimpleGraph::findNodeID(_graph, "Adam");
    const NodeID ghostsID    = SimpleGraph::findNodeID(_graph, "Ghosts");
    const NodeID computersID = SimpleGraph::findNodeID(_graph, "Computers");
    const NodeID bioID       = SimpleGraph::findNodeID(_graph, "Bio");

    // Adam and Ghosts each appear twice (once as Remy's neighbour via each edge direction).
    EXPECT_GE(std::count(results.begin(), results.end(), adamID), 2);
    EXPECT_GE(std::count(results.begin(), results.end(), ghostsID), 2);
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), remyID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), computersID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), bioID));
}

TEST_F(IndexQueriesTest, ageIndexUsedInReverseTraversal) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX ageindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    ASSERT_TRUE(hasIndex("ageindex"));

    const std::vector<NodeID> results =
        collectNodes("MATCH (n)<--(m) WHERE n.age = 32 RETURN m", "m");

    ASSERT_EQ(3u, results.size());

    const NodeID remyID   = SimpleGraph::findNodeID(_graph, "Remy");
    const NodeID adamID   = SimpleGraph::findNodeID(_graph, "Adam");
    const NodeID ghostsID = SimpleGraph::findNodeID(_graph, "Ghosts");

    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), remyID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), adamID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), ghostsID));
}

TEST_F(IndexQueriesTest, ageIndexTypedTwoHopTraversal) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX ageindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    ASSERT_TRUE(hasIndex("ageindex"));

    const std::vector<NodeID> results =
        collectNodes("MATCH (n)-[:KNOWS_WELL]->(m)-->(p) WHERE n.age = 32 RETURN p", "p");

    ASSERT_EQ(7u, results.size());

    const NodeID remyID      = SimpleGraph::findNodeID(_graph, "Remy");
    const NodeID adamID      = SimpleGraph::findNodeID(_graph, "Adam");
    const NodeID ghostsID    = SimpleGraph::findNodeID(_graph, "Ghosts");
    const NodeID computersID = SimpleGraph::findNodeID(_graph, "Computers");
    const NodeID bioID       = SimpleGraph::findNodeID(_graph, "Bio");
    const NodeID cookingID   = SimpleGraph::findNodeID(_graph, "Cooking");

    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), remyID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), adamID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), ghostsID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), computersID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), bioID));
    EXPECT_NE(results.end(), std::find(results.begin(), results.end(), cookingID));
}

TEST_F(IndexQueriesTest, dropIndexRemovedAfterCommit) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX dropme FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_EQ(1, countIndexes());

    ASSERT_TRUE(query("DROP INDEX dropme", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    EXPECT_EQ(0, countIndexes());
    EXPECT_FALSE(hasIndex("dropme"));
}

TEST_F(IndexQueriesTest, dropIndexRemovedAfterSubmit) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX dropme FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();
    ASSERT_EQ(1, countIndexes());

    newChange();
    ASSERT_TRUE(query("DROP INDEX dropme", emptyCallback));
    submitCurrentChange();

    EXPECT_EQ(0, countIndexes());
    EXPECT_FALSE(hasIndex("dropme"));
}

TEST_F(IndexQueriesTest, dropIndexOnlyRemovesNamedIndex) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX keepme FOR (n) ON n.name", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_TRUE(query("CREATE INDEX dropme FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    submitCurrentChange();
    ASSERT_EQ(2, countIndexes());

    newChange();
    ASSERT_TRUE(query("DROP INDEX dropme", emptyCallback));
    submitCurrentChange();

    EXPECT_EQ(1, countIndexes());
    EXPECT_TRUE(hasIndex("keepme"));
    EXPECT_FALSE(hasIndex("dropme"));
}

TEST_F(IndexQueriesTest, droppedIndexNotVisibleBeforeCommit) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX dropme FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_EQ(1, countIndexes());

    // DROP without committing — index should still be visible
    ASSERT_TRUE(query("DROP INDEX dropme", emptyCallback));
    EXPECT_EQ(1, countIndexes());
    EXPECT_TRUE(hasIndex("dropme"));

    ASSERT_TRUE(query("commit", emptyCallback));
    EXPECT_EQ(0, countIndexes());
    EXPECT_FALSE(hasIndex("dropme"));
}

TEST_F(IndexQueriesTest, recreateIndexAfterDrop) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX myindex FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_TRUE(query("DROP INDEX myindex", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    submitCurrentChange();

    ASSERT_EQ(0, countIndexes());

    newChange();
    ASSERT_TRUE(query("CREATE INDEX myindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    EXPECT_EQ(1, countIndexes());
    EXPECT_TRUE(hasIndex("myindex"));
}

TEST_F(IndexQueriesTest, dropEdgeIndex) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX durationidx FOR [e] ON e.duration", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_EQ(1, countIndexes());

    ASSERT_TRUE(query("DROP INDEX durationidx", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    EXPECT_EQ(0, countIndexes());
    EXPECT_FALSE(hasIndex("durationidx"));
}

// A drop in change1 survives rebase when change2 (submitted first) is unrelated.
TEST_F(IndexQueriesTest, dropIndexSurvivesUnrelatedRebase) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX dropme FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_TRUE(query("DROP INDEX dropme", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    EXPECT_EQ(0, countIndexes());
    EXPECT_FALSE(hasIndex("dropme"));

    newChange(), change2 = _currentChange;
    // Unrelated write — creates a node with a property not covered by the index.
    ASSERT_TRUE(query("CREATE (n:TestNode { val: 1 })", emptyCallback));

    // Submit change2 first so change1 must rebase against it.
    submitChange(change2);
    submitChange(change1);

    EXPECT_EQ(0, countIndexes());
    EXPECT_FALSE(hasIndex("dropme"));
}

TEST_F(IndexQueriesTest, createRebasedOnTopOfDrop) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX myindex FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX myindex FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_TRUE(query("DROP INDEX myindex", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    submitChange(change2);
    submitChange(change1);

    EXPECT_EQ(1, countIndexes());
    EXPECT_TRUE(hasIndex("myindex"));
}

// change1 drops an index; change2 (submitted first) creates an unrelated index.
// After rebase both the drop and the unrelated index should be reflected.
TEST_F(IndexQueriesTest, dropAndUnrelatedIndexMergedAfterRebase) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX ageidx FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));
    ASSERT_TRUE(query("DROP INDEX ageidx", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX nameidx FOR (n) ON n.name", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    // Submit change2 first — head gains nameidx.
    // change1 rebases: its drop of ageidx should survive; nameidx from the head should be kept.
    submitChange(change2);
    submitChange(change1);

    EXPECT_FALSE(hasIndex("ageidx"));
    EXPECT_TRUE(hasIndex("nameidx"));
}

TEST_F(IndexQueriesTest, ageIndexReturnPropertyOnIndexedNode) {
    newChange();
    ASSERT_TRUE(query("CREATE INDEX ageindex FOR (n) ON n.age", emptyCallback));
    submitCurrentChange();

    ASSERT_TRUE(hasIndex("ageindex"));

    std::vector<std::string> names;
    auto res = query("MATCH (n) WHERE n.age = 32 RETURN n.name", [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* col = findColumn(df, "n.name");
        ASSERT_TRUE(col) << "Column 'n.name' not found";
        auto* vec = col->as<ColumnOptVector<types::String::Primitive>>();
        ASSERT_TRUE(vec);
        for (auto name : *vec) {
            ASSERT_TRUE(name);
            names.emplace_back(*name);
        }
    });
    EXPECT_TRUE(res) << res.getError();

    ASSERT_EQ(2u, names.size());
    EXPECT_NE(names.end(), std::find(names.begin(), names.end(), "Remy"));
    EXPECT_NE(names.end(), std::find(names.begin(), names.end(), "Adam"));
}
