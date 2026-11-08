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

// The nl program a query lowers to is the same however the ops lowering creates and erases
// land on the heap. Each lowering runs behind padding of another size, which shifts the
// addresses of everything it allocates.
class LoweringDeterminismTest : public TuringTest {
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
    static constexpr size_t LOWERING_COUNT = 256;
    static constexpr size_t PADDING_STEP = 24;

    void loweredDump(std::string_view query, std::string& dump) {
        const std::string explained = "EXPLAIN (nl) " + std::string(query);

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
            if (row.front() == "nl") {
                dump = row.back();
            }
        }

        ASSERT_FALSE(dump.empty()) << "query: " << query;
    }

    void expectOneProgram(std::string_view query) {
        std::string first;
        ASSERT_NO_FATAL_FAILURE(loweredDump(query, first));

        std::vector<std::unique_ptr<char[]>> padding;
        for (size_t lowering = 1; lowering < LOWERING_COUNT; lowering++) {
            padding.push_back(std::make_unique<char[]>(lowering * PADDING_STEP));

            std::string dump;
            ASSERT_NO_FATAL_FAILURE(loweredDump(query, dump));
            ASSERT_EQ(dump, first) << "query: " << query << "\nlowering " << lowering;
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoweringDeterminismTest, laysOutAConstantOfAReduceWhoseAccumulatorWidens) {
    expectOneProgram("MATCH (n:Person) "
                     "RETURN n.name, reduce(line = '', x IN range(0, 3) | "
                     "line + coalesce(head([t IN [[0, 'a'], [2, 'c']] WHERE t[0] = x | t[1]]), '#'))");
}

TEST_F(LoweringDeterminismTest, laysOutACaseConstantOfAReduceWhoseAccumulatorWidens) {
    expectOneProgram("MATCH (n:Person) "
                     "RETURN n.name, reduce(total = 0, x IN range(1, n.age % 5) | "
                     "total + CASE WHEN x > 2 THEN 0.5 ELSE 1 END)");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
