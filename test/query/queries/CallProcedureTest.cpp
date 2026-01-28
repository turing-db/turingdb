#include <gtest/gtest.h>

#include "SystemManager.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
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

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
};

TEST_F(CallProcedureTest, Labels) {
    bool executed = false;
    const auto res = _db->query(
        "CALL db.labels()", "simpledb", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 2);
            ASSERT_EQ(df->getRowCount(), 9);

            executed = true;
        });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, EdgeTypes) {
    bool executed = false;
    const auto res = _db->query(
        "CALL db.edgeTypes()", "simpledb", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 2);
            ASSERT_EQ(df->getRowCount(), 2);

            executed = true;
        });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, PropertyTypes) {
    bool executed = false;
    const auto res = _db->query(
        "CALL db.propertyTypes()", "simpledb", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 3);
            ASSERT_EQ(df->getRowCount(), 8);

            executed = true;
        });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, History) {
    bool executed = false;
    const auto res = _db->query(
        "CALL db.propertyTypes()", "simpledb", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 3);
            ASSERT_EQ(df->getRowCount(), 8);

            executed = true;
        });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

TEST_F(CallProcedureTest, DescribeCommit) {
    bool executed = false;
    const auto res = _db->query(
        "CALL db.history() YIELD commit AS c "
        "CALL db.describeCommit(c) YIELD nodeCount, edgeCount "
        "RETURN nodeCount, edgeCount",
        "simpledb",
        &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 2);
            ASSERT_EQ(df->getRowCount(), 8);

            const auto& cols = df->cols();

            const auto* nodeCounts = cols.at(0)->as<ColumnVector<uint64_t>>();
            const auto* edgeCounts = cols.at(1)->as<ColumnVector<uint64_t>>();

            ASSERT_TRUE(nodeCounts != nullptr);
            ASSERT_TRUE(edgeCounts != nullptr);

            const auto check = [&](size_t i, uint64_t nodeCount, uint64_t edgeCount) {
                EXPECT_EQ(nodeCounts->at(i), nodeCount);
                EXPECT_EQ(edgeCounts->at(i), edgeCount);
            };

            check(0, 0, 0);
            check(1, 7, 8);
            check(2, 2, 2);
            check(3, 2, 2);
            check(4, 1, 1);
            check(5, 1, 0);
            check(6, 3, 2);
            check(7, 2, 3);

            executed = true;
        });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
