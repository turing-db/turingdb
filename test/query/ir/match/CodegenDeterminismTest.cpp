#include <gtest/gtest.h>

#include <stddef.h>

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

// The program a query compiles to is the same however its declarations and variables land
// on the heap. Each compilation runs behind padding of another size, which shifts the
// addresses of everything the compiler allocates.
class CodegenDeterminismTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        SystemManager& systemManager = _env->getSystemManager();

        SystemAccessor system = systemManager.accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&systemManager, &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    static constexpr size_t COMPILATION_COUNT = 16;
    static constexpr size_t PADDING_STEP = 72;

    void codegenDump(std::string_view query, std::string& dump) {
        const std::string explained = "EXPLAIN (codegen) " + std::string(query);

        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              explained,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "codegen") {
                dump = row.back();
            }
        }

        ASSERT_FALSE(dump.empty()) << "query: " << query;
    }

    void expectOneProgram(std::string_view query) {
        std::string first;
        ASSERT_NO_FATAL_FAILURE(codegenDump(query, first));

        std::vector<std::unique_ptr<char[]>> padding;
        for (size_t compilation = 1; compilation < COMPILATION_COUNT; compilation++) {
            padding.push_back(std::make_unique<char[]>(compilation * PADDING_STEP));

            std::string dump;
            ASSERT_NO_FATAL_FAILURE(codegenDump(query, dump));
            ASSERT_EQ(dump, first) << "query: " << query << "\ncompilation " << compilation;
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CodegenDeterminismTest, carriesTheColumnsOfACycleInOneOrder) {
    expectOneProgram("MATCH (a)-->(b)-->(a) RETURN a, b ORDER BY a, b");
}

TEST_F(CodegenDeterminismTest, carriesTheColumnsOfSeveralPatternsInOneOrder) {
    expectOneProgram("MATCH (y)-->(z)-->(m), (n), (l)-->(m), (n)-->(p)-->(q) WHERE m.age < p.age RETURN n, m, z, p");
    expectOneProgram("MATCH (n)-->(p)--(z), (p)-->(m)--(w)-->(u) "
                     "WHERE n.age <> u.age + 10 AND z.age = u.age + 2 + m.age * m.age + 10 RETURN n, m, z, p");
    expectOneProgram("MATCH (n)-->(p)--(z), (U)-->(J)--(x)--(z), (U)-->(m)--(w)-->(u) "
                     "WHERE n.age <> u.age - m.age * m.age + 10 AND z.age = u.age + 2 RETURN n, m, z, u");
}

TEST_F(CodegenDeterminismTest, joinsTheOccurrencesOfEachEdgeInOneOrder) {
    expectOneProgram("MATCH (a)-[e]->(b), (c)-[e]->(d), (a)-[f]->(c), (b)-[f]->(d) RETURN a, b, c, d");
}

TEST_F(CodegenDeterminismTest, bindsTheVariablesOfSeveralMatchesInOneOrder) {
    expectOneProgram("MATCH (n)-->(z), (l)-->(m) MATCH (n)-->(p)-->(q) WHERE m = p.age / -1 AND m.age = p.age + 28 RETURN n, m, z, p");
    expectOneProgram("MATCH (n)-->(z)-->(m) MATCH (n), (l)-->(m) MATCH (n)-->(p)-->(q) "
                     "WHERE n = p.age / 10 AND m.age < p.age + 2.8 RETURN n, m, z, p");
}

TEST_F(CodegenDeterminismTest, carriesAGroupingKeyReadTwiceByAComprehensionInOneOrder) {
    expectOneProgram("MATCH (n:Person) RETURN n.age, [x IN collect(n.name) | toString(n.age) + x + toString(n.age + 1)]");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
