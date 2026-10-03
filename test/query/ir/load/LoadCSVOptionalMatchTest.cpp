#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
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

namespace {

constexpr std::string_view PEOPLE_FILE = "Remy,red\n"
                                         "Adam,blue\n"
                                         "Nobody,green\n";

constexpr std::string_view HEADED_FILE = "who,colour\n"
                                         "Remy,red\n"
                                         "Adam,blue\n"
                                         "Nobody,green\n";

}

class LoadCSVOptionalMatchTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        writeFile("people.csv", PEOPLE_FILE);
        writeFile("headed.csv", HEADED_FILE);
    }

protected:
    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
        file.close();
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::ranges::sort(sortedExpected);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected) << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoadCSVOptionalMatchTest, readsAHeaderFieldInThePattern) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person {name: row.who}) "
               "RETURN row.who, p.name",
               {{"Remy", "Remy"}, {"Adam", "Adam"}, {"Nobody", "null"}});
}

TEST_F(LoadCSVOptionalMatchTest, readsAnIndexedFieldInThePattern) {
    expectRows("LOAD CSV 'people.csv' AS row OPTIONAL MATCH (p:Person {name: row[0]}) "
               "RETURN row[0], p.name",
               {{"Remy", "Remy"}, {"Adam", "Adam"}, {"Nobody", "null"}});
}

TEST_F(LoadCSVOptionalMatchTest, readsAFieldInAWhere) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person) WHERE p.name = row.who "
               "RETURN row.who, p.name",
               {{"Remy", "Remy"}, {"Adam", "Adam"}, {"Nobody", "null"}});
}

TEST_F(LoadCSVOptionalMatchTest, keepsEveryFieldPastThePattern) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person {name: row.who}) "
               "RETURN row.who, row.colour, p.name",
               {{"Remy", "red", "Remy"}, {"Adam", "blue", "Adam"}, {"Nobody", "green", "null"}});
}

TEST_F(LoadCSVOptionalMatchTest, keepsAFieldThePatternDoesNotRead) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person {name: 'Remy'}) "
               "RETURN row.colour, p.name",
               {{"red", "Remy"}, {"blue", "Remy"}, {"green", "Remy"}});
}
