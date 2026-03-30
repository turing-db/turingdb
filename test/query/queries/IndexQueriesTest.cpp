#include <gtest/gtest.h>

#include <cstdint>
#include <string_view>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/Transaction.h"
#include "dataframe/Dataframe.h"
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

    // Return the name column from the showIndexes Dataframe
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

    void submitCurrentChange() {
        auto res = _db->query("CHANGE SUBMIT", _graphName, &_env->getMem(), &_queryConfig,
                                emptyCallback, CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void submitChange(ChangeID chid) {
        auto res = _db->query("CHANGE SUBMIT", _graphName, &_env->getMem(), &_queryConfig,
                                emptyCallback, CommitHash::head(), chid);
        ASSERT_TRUE(res) << res.getError();
        _currentChange = ChangeID::head();
    }

    void setChange(ChangeID chid) {
        _currentChange = chid;
    }

    auto query(std::string_view q, auto callback) {
        return _db->query(q, _graphName, &_env->getMem(), &_queryConfig, callback,
                          CommitHash::head(), _currentChange);
    }

    // Counts the total number of indexes visible at the current head.
    // Returns 0 if the callback is never invoked (empty result set).
    size_t countIndexes() {
        size_t count = 0;
        auto res = query("CALL db.showIndexes() YIELD name", [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            count += df->getLogicalRowCount();
        });
        EXPECT_TRUE(res) << res.getError();
        return count;
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

TEST_F(IndexQueriesTest, indexGoneAfterRebaseWriteBuffer) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));

    newChange(), change2 = _currentChange;
    ASSERT_TRUE(query("CREATE (n:RebaseTestNode { val: 99 })", emptyCallback));

    // Submit change2 first so that change1 must rebase when submitted
    submitChange(change2);
    submitChange(change1);

    EXPECT_EQ(0, countIndexes())
        << "After rebase, all indexes should be reset and showIndexes should return 0 rows";
}

TEST_F(IndexQueriesTest, indexGoneAfterRebaseDataPart) {
    ChangeID change1, change2;

    newChange(), change1 = _currentChange;
    ASSERT_TRUE(query("CREATE INDEX rebaseindex FOR (n) ON n.age", emptyCallback));
    ASSERT_TRUE(query("commit", emptyCallback));

    newChange(), change2 = _currentChange;
    ASSERT_TRUE(query("CREATE (n:RebaseTestNode { val: 99 })", emptyCallback));

    // Submit change2 first so that change1 must rebase when submitted
    submitChange(change2);
    submitChange(change1);

    EXPECT_EQ(0, countIndexes())
        << "After rebase, all indexes should be reset and showIndexes should return 0 rows";
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

