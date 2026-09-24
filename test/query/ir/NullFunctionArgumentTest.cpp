#include <gtest/gtest.h>

#include <algorithm>
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

class NullFunctionArgumentTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(NullFunctionArgumentTest, returnsNullForTheSizeOfNull) {
    expectRows("RETURN size(null)", {{"null"}});
}

TEST_F(NullFunctionArgumentTest, returnsNullForTheLengthOfNull) {
    expectRows("RETURN length(null)", {{"null"}});
}

TEST_F(NullFunctionArgumentTest, returnsNullForTheIntegerOfNull) {
    expectRows("RETURN toInteger(null)", {{"null"}});
}

TEST_F(NullFunctionArgumentTest, returnsNullForTheSizeOfAVariableBoundToNull) {
    expectRows("WITH null AS s RETURN size(s)", {{"null"}});
}

TEST_F(NullFunctionArgumentTest, returnsNullForTheSizeOfAnAbsentProperty) {
    expectRows("MATCH (n:Person) RETURN n.name, size(n.dob)",
               {{"Remy", "5"}, {"Adam", "5"}, {"Maxime", "5"}, {"Luc", "5"},
                {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
