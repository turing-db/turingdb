#include <gtest/gtest.h>

#include <string_view>
#include <cstdint>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/Transaction.h"
#include "versioning/CommitData.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class MergeDataPartsTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        _db = &_env->getDB();
    }

protected:
    std::string _graphName = "mergetest";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;
    ChangeID _currentChange {ChangeID::head()};

    static constexpr auto emptyCallback = [](const Dataframe*) -> void {};

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);
        Change* change = res.value();
        _currentChange = change->id();
    }

    auto query(std::string_view query, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        return _db->query(query, _graphName, &_env->getMem(), &_queryConfig, callbacks,
                          CommitHash::head(), _currentChange);
    }

    void submitCurrentChange() {
        auto res = query("CHANGE SUBMIT", emptyCallback);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }

    size_t getTotalDataPartCount() {
        auto tx = _graph->openTransaction();
        return tx.commitData()->allDataparts().size();
    }
};

TEST_F(MergeDataPartsTest, mergeReducesPartCount) {
    // Create two separate changes, each with their own data.
    // Each submitted change adds a data part, so we end up with multiple parts.

    {
        newChange();
        ASSERT_TRUE(query(
            R"(CREATE (n:Person { name: "Alice" })-[e:KNOWS { since: 2020 }]->(m:Person { name: "Bob" }))",
            emptyCallback));
        submitCurrentChange();
    }

    {
        newChange();
        ASSERT_TRUE(query(
            R"(CREATE (n:Person { name: "Charlie" })-[e:KNOWS { since: 2021 }]->(m:Person { name: "Diana" }))",
            emptyCallback));
        submitCurrentChange();
    }

    // Before merge: should have more than 1 data part
    const size_t partsBefore = getTotalDataPartCount();
    ASSERT_GT(partsBefore, 1u) << "Expected multiple data parts before merge";

    // Run MERGE DATA PARTS
    auto mergeRes = query("MERGE_DATAPARTS", emptyCallback);
    ASSERT_TRUE(mergeRes) << mergeRes.getError();

    // After merge: should have exactly 1 data part
    const size_t partsAfter = getTotalDataPartCount();
    ASSERT_EQ(1u, partsAfter) << "Expected exactly 1 data part after merge";
}

TEST_F(MergeDataPartsTest, mergePreservesData) {
    // Create data across multiple changes
    {
        newChange();
        ASSERT_TRUE(query(
            R"(CREATE (n:Person { name: "Alice" })-[e:KNOWS { since: 2020 }]->(m:Person { name: "Bob" }))",
            emptyCallback));
        submitCurrentChange();
    }

    {
        newChange();
        ASSERT_TRUE(query(
            R"(CREATE (n:Person { name: "Charlie" })-[e:KNOWS { since: 2021 }]->(m:Person { name: "Diana" }))",
            emptyCallback));
        submitCurrentChange();
    }

    // Verify multiple parts exist
    ASSERT_GT(getTotalDataPartCount(), 1u);

    // Collect data before merge
    using Name = types::String::Primitive;
    using Rows = LineContainer<Name, Name>;

    auto collectData = [&]() {
        Rows rows;
        auto res = query(
            "MATCH (n:Person)-[e:KNOWS]->(m:Person) RETURN n.name, m.name",
            [&](const Dataframe* df) {
                ASSERT_TRUE(df);

                auto* nname = findColumn(df, "n.name")->as<ColumnOptVector<Name>>();
                auto* mname = findColumn(df, "m.name")->as<ColumnOptVector<Name>>();

                ASSERT_TRUE(nname);
                ASSERT_TRUE(mname);
                ASSERT_EQ(nname->size(), mname->size());

                for (size_t i = 0; i < nname->size(); ++i) {
                    ASSERT_TRUE(nname->at(i).has_value());
                    ASSERT_TRUE(mname->at(i).has_value());
                    rows.add({*nname->at(i), *mname->at(i)});
                }
            });

        EXPECT_TRUE(res);
        return rows;
    };

    Rows beforeMerge = collectData();

    // Run MERGE DATA PARTS
    auto mergeRes = query("MERGE_DATAPARTS", emptyCallback);
    ASSERT_TRUE(mergeRes) << mergeRes.getError();

    // Verify merge happened
    ASSERT_EQ(1u, getTotalDataPartCount());

    // Collect data after merge
    Rows afterMerge = collectData();

    // Data must be identical
    ASSERT_TRUE(beforeMerge.equals(afterMerge))
        << "Data changed after merge";
}
