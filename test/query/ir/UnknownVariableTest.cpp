#include <gtest/gtest.h>

#include <memory>
#include <span>
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

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class RowCountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

}

// The query test suite's fail-reads-match-1 case on the v3 engine. A name no pattern binds
// stands for nothing the query can read, so it is turned away wherever it is read from -
// through a property, on its own, or as the subject of a label test.
class UnknownVariableTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectUnknownVariableRejection(std::string_view query, std::string_view reason) {
        RowCountingSink sink;
        QueryStatus status;
        runQuery(query, status, sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << "query: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowCountingSink sink;
        QueryStatus status;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getRowCount(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }
};

// fail-reads-match-1: 'a' is read for a property in a WHERE comparing the two patterns
TEST_F(UnknownVariableTest, rejectsAnUnknownVariableReadForAProperty) {
    expectUnknownVariableRejection("MATCH (n) MATCH (m) WHERE n.duration = a.duration RETURN n, m;",
                                   "Variable 'a' not found");
}

// The same name read on its own, where the projection asks for the entity rather than one
// of its properties
TEST_F(UnknownVariableTest, rejectsAnUnknownVariableReturnedOnItsOwn) {
    expectUnknownVariableRejection("MATCH (n) RETURN n, a;", "Variable 'a' not found");
}

// The same name as the subject of a label test
TEST_F(UnknownVariableTest, rejectsAnUnknownVariableTestedForALabel) {
    expectUnknownVariableRejection("MATCH (n) WHERE a:Person RETURN n;", "Variable 'a' not found");
}

// The valid neighbour of those: the WHERE reads a property of the second pattern instead of
// an unbound name. Remy and Adam are the only nodes carrying an age, and both are 32, so
// every ordered pair of them matches.
TEST_F(UnknownVariableTest, joinsTwoPatternsOnAPropertyOfADeclaredVariable) {
    expectRowCount("MATCH (n) MATCH (m) WHERE n.age = m.age RETURN n, m;", 4);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
