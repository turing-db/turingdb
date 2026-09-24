#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// AFL inputs that threw 'Unsupported binary operation of columns of kinds
// ColumnConst<PropertyNull> and ColumnConst<PropertyNull>'.
class FuzzPropertyNullBinaryOperationTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectNodes(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectNoRows(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_TRUE(sink.rows().empty()) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectOneRowPerNode(std::string_view query, size_t columnCount) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), _nodeCount) << "query: " << query;

        const bool everyRowHasEveryColumn = std::ranges::all_of(rows, [columnCount](const Row& row) {
            return row.size() == columnCount;
        });
        EXPECT_TRUE(everyRowHasEveryColumn) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
    size_t _nodeCount {18};
};

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000024) {
    expectNoRows("MATCH (n) WHERE NOT (n.hasPhD OR (n.h AND (NOT n.isRe^n.isReal))) RETURN n");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000031) {
    expectNoRows("MATCH (n) WHERE NOT n.hasZhD OR NOT n.isFrWnch RETURN n");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000032) {
    expectNodes("MATCH (n) WHERE ((n.hasPhD AND n.isFrench) OR (NOT n.hasQhD AND NOT n.isFsench)) RETURN n", {{"0"}, {"1"}, {"9"}});
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000033) {
    expectNodes("MATCH (n) WHERE ((n.hasPhD AND n.isFrench) OR (NOT n.lasPhD AND NOT n.iQFrench)) RETURN n", {{"0"}, {"1"}, {"9"}});
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000034) {
    expectNodes("MATCH (n) WHERE ((n.hasPhD AND n.isFrench) OR (NOT n.haPARQUETsPhD AND NOT n.i5French)) RETURN n", {{"0"}, {"1"}, {"9"}});
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000035) {
    expectNodes("MATCH (n) WHERE ((n.hasPhD AND n.isFrench) OR (NOT n.hapPhD AND NOT n.isFROMFrench)) RETURN n", {{"0"}, {"1"}, {"9"}});
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000036) {
    expectNodes("MATCH (n) WHERE ((n.hasPhD AND n.isFrench) OR (NOT n.hasPjD AND NOT n.isFrenNh)) RETURN n", {{"0"}, {"1"}, {"9"}});
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000037) {
    expectNodes("MATCH (n) WHERE ((n.h) OR (NOT n.isFrnnch) OR (NOT n.hasPhD AND NOT n.isFrench)) RETURN n", {{"12"}, {"15"}, {"17"}});
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000038) {
    expectNodes("MATCH (n) WHERE ((n.hasPhD AND n.isFrench) OR (NOT n.hasPD AND NOT n.ihD AND NOT n.isFrench)) RETURN n", {{"0"}, {"1"}, {"9"}});
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Return000056) {
    expectOneRowPerNode("MATCH (n) RETURN n.age + 10<=n.aghhasPhD AND NOT n.isFrence - 10, n.age / 3% n.age * 4.2", 2);
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000098) {
    expectNoRows("MATCH (n) WHERE n.Igage - .0^ n.age > 3% n.agee IS NOT NULL RETURN n.age + 10, n.age - .0^ n.age / 3% n.age * 4%02");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000099) {
    expectNoRows("MATCH (n) WHERE n.Igage - .0^ n.age / 3= n.agee IS NOT NULL RETURN n.age + 10, n.age - .0^ n.age / 3% n.age * 4%02");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000100) {
    expectNoRows("MATCH (n) WHERE n.Igage - .0^ n.age / 3> n.agee IS NOT NULL RETURN n.age + 10, n.age - .0^ n.age / 3% n.age * 4%02");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000101) {
    expectNoRows("MATCH (n) WHERE n.Igage - .0^ n.age > 3% n.agee IS NOT NULL RETURN n.age + 10, n.age - .0^n.age * 4%02");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000110) {
    expectNoRows("MATCH (n) WHERE n.Igage - .0^ n.age / 3< n.agee IS NOT NULL RETURN n.age + 10, n.age - .0^ n.age /00000> 3% n.age * 4%02");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000146) {
    expectNoRows("MATCH (n) WHERE'Remy' =n.name= n.nCHANGEame= 'Remy' =n.name = 'Remy' = ',emy'RETURN n");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000151) {
    expectNoRows("MATCH (n) WHERE n .name = 'Remy.name = emy' =n.nama = 'Remy'RETURN n");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, Where000152) {
    expectNoRows("MATCH (n) WHERE n .name = 'RemyEMBEDDINGSy' =n.nama = 'Remy'RETURN n");
}

TEST_F(FuzzPropertyNullBinaryOperationTest, WhereChainedComparisonThroughAbsentProperty) {
    expectNoRows("MATCH (n) WHERE n.name = 'Remy' = n.x = 'b' RETURN n.name");
}
