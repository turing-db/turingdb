#include <gtest/gtest.h>

#include "SystemManager.h"
#include "TuringDB.h"
#include "QueryConfig.h"
#include "QueryStatus.h"
#include "SimpleGraph.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class CallProcedureTest : public TuringTest {
public:
    void initialize() override {
        const auto testTuringDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(testTuringDir);
        Graph* graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(graph);
        _db = &_env->getDB();
    }

    auto query(std::string_view q, std::string_view graphName, auto callback) {
        db::QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const db::QueryState state(graphName, &_env->getMem(), &_queryConfig, &callbacks);
        return _db->query(q, state);
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(CallProcedureTest, Labels) {
    bool executed = false;
    const auto res = query("CALL db.labels()", "simpledb", [&](const Dataframe* df) -> void {
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
    const auto res = query("CALL db.edgeTypes()", "simpledb", [&](const Dataframe* df) -> void {
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
    const auto res = query("CALL db.propertyTypes()", "simpledb", [&](const Dataframe* df) -> void {
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
    const auto res = query("CALL db.propertyTypes()", "simpledb", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 3);
            ASSERT_EQ(df->getLogicalRowCount(), 8);

            executed = true;
        });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, DescribeCommit) {
    bool executed = false;
    const auto res = query("CALL db.history() YIELD commit AS c "
        "CALL db.describeCommit(c) YIELD nodeCount, edgeCount "
        "RETURN nodeCount, edgeCount", "simpledb", [&](const Dataframe* df) -> void {
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
        "WHERE a = b RETURN count(*)", "simpledb", [&](const Dataframe* df) -> void {
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
        "WHERE a = b RETURN count(*)", "simpledb", [&](const Dataframe* df) -> void {
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
        "WHERE l = m RETURN count(*)", "simpledb", [&](const Dataframe* df) -> void {
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
        "WHERE l = p RETURN count(*)", "simpledb", [&](const Dataframe*) -> void {});

    EXPECT_EQ(res.getStatus(), QueryStatus::Status::ANALYZE_ERROR);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
