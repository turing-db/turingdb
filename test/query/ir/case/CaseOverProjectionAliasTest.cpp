#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// Every WHEN and THEN of a CASE reads a RETURN or WITH alias at the row being decided,
// whichever branch that row reaches.
class CaseOverProjectionAliasTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void expectOrderedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CaseOverProjectionAliasTest, ordersOnACaseOverThePropertyItAliases) {
    expectOrderedRows("MATCH (n:Person) RETURN n.name AS name ORDER BY CASE WHEN n.name = 'Remy' THEN '' ELSE n.name END",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}

TEST_F(CaseOverProjectionAliasTest, ordersOnAnElseReadingTheReturnAlias) {
    expectOrderedRows("MATCH (n:Person) RETURN n.name AS name ORDER BY CASE WHEN name = 'Remy' THEN '' ELSE name END",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}

TEST_F(CaseOverProjectionAliasTest, ordersOnASecondWhenReadingTheReturnAlias) {
    expectOrderedRows("MATCH (n:Person) RETURN n.name AS name "
                      "ORDER BY CASE WHEN name = 'Remy' THEN 0 WHEN name = 'Adam' THEN 1 ELSE 2 END, name",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}

TEST_F(CaseOverProjectionAliasTest, ordersOnASecondWhenReadingTheWithAlias) {
    expectOrderedRows("MATCH (n:Person) WITH n.name AS name "
                      "ORDER BY CASE WHEN name = 'Remy' THEN 0 WHEN name = 'Adam' THEN 1 ELSE 2 END, name RETURN name",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}

TEST_F(CaseOverProjectionAliasTest, ordersOnAComprehensionReadingTheReturnAlias) {
    expectOrderedRows("MATCH (n:Person) RETURN n.name AS name ORDER BY [x IN [1, 2] | name][0]",
                      {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
