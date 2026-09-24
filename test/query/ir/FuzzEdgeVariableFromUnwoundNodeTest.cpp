#include <gtest/gtest.h>

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

// AFL inputs that failed the assertion '_part._varMap.contains(representative)' in
// DBProgramGenerator: a MATCH that names its edge, from a node an UNWIND took out of a list.
class FuzzEdgeVariableFromUnwoundNodeTest : public TuringTest {
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

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(FuzzEdgeVariableFromUnwoundNodeTest, Match000003) {
    expectRows("MATCH (p:Person) WITH collect(p) AS people UNWIND people AS person MATCH (person)-[ZINTERESTED_IN]->(i) RETURN i.name",
               {{"Adam"},
                {"Animals"},
                {"Bio"},
                {"Bio"},
                {"Computers"},
                {"Computers"},
                {"Cooking"},
                {"Cooking"},
                {"Eighties"},
                {"Ghosts"},
                {"Gym"},
                {"Gym"},
                {"Gym"},
                {"JiuJitsu"},
                {"Padel"},
                {"Remy"},
                {"Travel"}});
}

TEST_F(FuzzEdgeVariableFromUnwoundNodeTest, Match000009) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' WITH p.name AS person, collect(i) AS interests WITH collect(interests) AS nested UNWIND nested AS one UNWIND one AS interest MATCH (interest)<-[tere:INTERESTED_IN]-(fan:Person) RETURN interest.name, fan.name",
               {{"Bio", "Adam"},
                {"Bio", "Maxime"},
                {"Cooking", "Adam"},
                {"Cooking", "Martina"}});
}

TEST_F(FuzzEdgeVariableFromUnwoundNodeTest, OutgoingEdge) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH collect(p) AS people UNWIND people AS person MATCH (person)-[e:KNOWS_WELL]->(k) RETURN e.name, k.name",
               {{"Remy -> Adam", "Adam"}});
}

TEST_F(FuzzEdgeVariableFromUnwoundNodeTest, IncomingEdge) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH collect(p) AS people UNWIND people AS person MATCH (k)-[e:KNOWS_WELL]->(person) RETURN e.name, k.name",
               {{"Adam -> Remy", "Adam"},
                {"Ghosts -> Remy", "Ghosts"}});
}
