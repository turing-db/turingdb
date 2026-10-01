#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "CompilerContext.h"
#include "LocalMemory.h"
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

class CompilerContextReuseTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

protected:
    void runQuery(QueryInterpreterV3& interpreter,
                  LocalMemory* memory,
                  std::string_view query,
                  std::vector<StringRowSink::Row>& rows) {
        StringRowSink sink;
        QueryStatus status;

        interpreter.execute(status,
                            query,
                            _graphName,
                            CommitHash::head(),
                            ChangeID::head(),
                            memory,
                            &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        rows = sink.getRows();
    }

    void expectRowsOfAFreshContext(CompilerContext* compilerContext, LocalMemory* memory) {
        QueryInterpreterV3 interpreter(&_env->getSystemManager());
        interpreter.setCompilerContext(compilerContext);

        for (size_t round = 0; round < 2; round++) {
            for (const std::string_view query : _queries) {
                QueryInterpreterV3 freshInterpreter(&_env->getSystemManager());
                std::vector<StringRowSink::Row> expected;
                runQuery(freshInterpreter, memory, query, expected);

                std::vector<StringRowSink::Row> rows;
                runQuery(interpreter, memory, query, rows);

                EXPECT_EQ(rows, expected) << "query: " << query;
            }
        }
    }

    const std::string _graphName = "simpledb";
    const std::vector<std::string_view> _queries {
        "RETURN 1",
        "RETURN 'Remy', [1, 2.5, 'x']",
        "MATCH (n {name: 'Remy'}) RETURN n",
        "MATCH (n) WHERE n.age > 30 RETURN n.name ORDER BY n.name",
        "EXPLAIN (codegen) MATCH (n) RETURN n.name",
    };
    std::unique_ptr<TuringTestEnv> _env;
};

TEST_F(CompilerContextReuseTest, answersLikeAFreshContext) {
    CompilerContext compilerContext;

    expectRowsOfAFreshContext(&compilerContext, &_env->getMem());
}

TEST_F(CompilerContextReuseTest, answersLikeAFreshContextWhenRebuiltForEveryQuery) {
    CompilerContext compilerContext(0);

    expectRowsOfAFreshContext(&compilerContext, &_env->getMem());
}

TEST_F(CompilerContextReuseTest, answersLikeAFreshContextOnEachThread) {
    std::vector<std::thread> threads;

    for (size_t threadIndex = 0; threadIndex < 2; threadIndex++) {
        threads.emplace_back([this] {
            LocalMemory memory;
            CompilerContext compilerContext;

            expectRowsOfAFreshContext(&compilerContext, &memory);
        });
    }

    for (std::thread& thread : threads) {
        thread.join();
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
