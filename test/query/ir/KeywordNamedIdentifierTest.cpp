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

// A word the grammar reads as a clause keyword is still a name a graph may use. The lexer
// is caseless, so the day a keyword is introduced every query that named a property, a
// variable or an alias after it stops parsing. Reaching all three means the keyword goes
// in the symbol rule: name derives symbol, so one entry covers the property and alias
// positions as well as the pattern variable that reservedWord never reaches.
class KeywordNamedIdentifierTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectParses(std::string_view query) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(KeywordNamedIdentifierTest, readsAPropertyNamedAfterAClauseKeyword) {
    expectParses("MATCH (n:Person) RETURN n.datetimes");
}

TEST_F(KeywordNamedIdentifierTest, bindsAVariableNamedAfterAClauseKeyword) {
    expectParses("MATCH (datetimes:Person) RETURN datetimes.name");
}

TEST_F(KeywordNamedIdentifierTest, projectsAnAliasNamedAfterAClauseKeyword) {
    expectParses("MATCH (n:Person) RETURN n.name AS datetimes");
}

TEST_F(KeywordNamedIdentifierTest, filtersOnAPropertyNamedAfterAClauseKeyword) {
    expectParses("MATCH (n:Person) WHERE n.datetimes IS NULL RETURN n.name");
}
