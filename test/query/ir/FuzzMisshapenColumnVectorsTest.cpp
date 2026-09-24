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

// AFL inputs that tripped the 'Misshapen ColumnVectors' assertion of BinaryPredicates.h.
class FuzzMisshapenColumnVectorsTest : public TuringTest {
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

    void expectNodesZeroAndOne(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        const Rows expected {{"0"}, {"1"}};
        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
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

TEST_F(FuzzMisshapenColumnVectorsTest, Where000064) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Rehy'!= 0 OR n = 1 OR n.nameame = 'Remy' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000065) {
    expectNodesZeroAndOne("MATCH (n) WHERE n.namen = 0 OR n = 1 OR n.naee = 'Remy' = 0 OR n = 1 OR n.name = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000066) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag< + 10<+ 10<=n.age - 10, n.ae - 10, n.age / 3% n.age * 4.2", 3);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000067) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag< + 10< + 10<=n.age - 10, n.ae - 10, n.age / 3% n.age * 4.2", 3);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000068) {
    expectOneRowPerNode("MATCH (n) RETURN n.cag< + 10< + 10<=n.age - 10, n.ae - 10, - 10, n.age / 3% n.age * 4.2", 4);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000069) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag<=n.ag< + 10< + 10<=n.age - 10, n.ae - 10, n.age / 3% n.age * 4.2", 3);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000071) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag< + 10<=n.ag< + 10<=0< 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000072) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag< + 10<10<=n.ag< + 10<=n.ag=n.ag< + 10<=n.age - 10, n.ae - n.ag< + 10< 3% n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000073) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag- n.ag< + 10< 3%10<=n.age - 10, n.ae - n.ag< + 10< 3% n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000085) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag% n.age * 00000>=00000004.200000>4000000+ 10<=n.age - 04.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000087) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag% n.age * 000000004.20000000>=00000004.200000>=000000<=0% n.age * 0000000044.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000092) {
    expectOneRowPerNode("MATCH (n) RETURN 0>=00000004.200000>=n.ENDage>+ .0000000000000>=00000000/ 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000093) {
    expectOneRowPerNode("MATCH (n) RETURN n.tge>+ .000000000000000000000000>=000>=00000000/ 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000094) {
    expectOneRowPerNode("MATCH (n) RETURN n.cge>+ .0000000000000>+ .0000000000000>=00000000/ 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000097) {
    expectOneRowPerNode("MATCH (n) RETURN n.ag< + 10<=n.ag< + 10<=n.ag +++++n.ag< + 10<=n.ag< + 10< 10<=n.age - 10+ 10<=n.ag + n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000102) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Remy' =n.name  OR n.nane = 'Remy' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000104) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Remy' =n.name  OR n.naie = 'Re(my' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000105) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0=n.na OR n = 1 OR n.name = 'Remy' =n.name  OR n.name6= 'Re(my' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000106) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Remy' = 0 OR n = 1 OR n.n= 'Re(my' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000108) {
    expectOneRowPerNode("MATCH (n) RETURN n.afe>+ .0000000<00000<=00000000/ 3% n.age - .0/ n.age ^ 3%  n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000109) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Remy' =n.name  OR n.nameeeeeeeeeeeeeeeeeeeeeeeeeee = 'remy' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000111) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0>=00000000000000000>0000000/ 3% n.ag00>=00000000000000000000>=00000000000000>0000000/ 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000112) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=00000000000>0000000/ 3% n.ag000000000>=00000000000000>0000000/ 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000113) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.aIe > .0<3> n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000114) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.DISTINCTage > .0<3> n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000115) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.Bge > .0<3> n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000116) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.ege > .0<3> n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000117) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.age0> .0<3< n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000118) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.Qgu > .0<3> n.age *0^ n.age /-4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000119) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.ageage > .0<3> n.age *0^ n.age /-4 > .0<3> n.age *0^ n.age /-4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000120) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.age > n.age *0^ n.a> .0<3> n.age *0^ n.age /-4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000121) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.agASe > .0<3> n.age *0^ n.age +-4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000122) {
    expectOneRowPerNode("MATCH (n) RETURN\tn.awe > .0<3> n.age^ n.age *0^ n.age /-4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000124) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Re\377\177' >n.name  OR n.namz = 'remy' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000125) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.nnnnnnnnnnnnnnnnnnname = 'Re\377\177' >n.name  OR n.namO = 'remy' = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000126) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age00000>=00000000000000>0000000000>0000000/ 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000127) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a00age > .0<3> n.ag0000>=00000000000000>0000000/ 3% n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000128) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a000000>=00000000000000>000000= 4 OR 0<=n.age - 10, 00/ 3% n.age * 4.2", 3);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000129) {
    expectOneRowPerNode("MATCH (n) RETURN 00000= 000= 4 OR 0<=n.age - 10, n.a000000>=00000000<00000>0000000/ 3% n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000130) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a0000>=0000= 4 OR 0<=n.age - 10, 00000>=00000000000000>0000000/ 3% n.age * 4.2", 3);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000131) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a000000>=00000000000000>0000000/ 300000>0000000/ 3% n.age % n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000132) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a000000>=000000000>=0000= 4 OR 0<=n.age - 10, 00000000>0000000/ 3% n.age * 4.2", 3);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000133) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a000000>=00000000000000>0<00000/ 3% n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000134) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a0>=0000= 4 OR 0<=n.age - 10, 0/ 3% n.age * 4.2", 3);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000135) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age - 10, n.a000000>=00000000000000>=0000= 4 OR 0<=n.age - 0000>0000000/ 3% n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000136) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.aGe - 10, n.aa000000>=0000000000000000>=00000000000000>0000000/ 3% n.age * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000137) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<=n.age000>[1, 2, 3]=00000000000000>0000000/ 3% n.age * 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000138) {
    expectOneRowPerNode("MATCH (n) RETURN 0000>=0000= 4 OR 0<0>0000000/3 % n.a00000>=0000000000000=n.age - 10, n.a0ge * 4.2", 2);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Where000139) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Re\377\177' >n.name  OR n.nJme = 'rem}' < 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000141) {
    expectOneRowPerNode("MATCH (n) RETURN n.afe>+ .0100000000000<=000000000000000<=00000000/ 3% n.age - 00/ 3% n.age - 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000142) {
    expectOneRowPerNode("MATCH (n) RETURN n.afe>+ .0000000+ .0000000000000<=000000<=00000000/ 3% n.age - 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000143) {
    expectOneRowPerNode("MATCH (n) RETURN n.afe>+000000<=0000000<=00000000/ 3% n.age - 4.2", 1);
}

TEST_F(FuzzMisshapenColumnVectorsTest, Return000144) {
    expectOneRowPerNode("MATCH (n) RETURN n.afe>+ .0>+ .00000000<=00000000/ 3% n.age - 4.2", 1);
}
