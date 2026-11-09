#include <stdlib.h>

#include <memory>
#include <stdexcept>

#include "Graph.h"
#include "ID.h"
#include "JobSystem.h"
#include "metadata/PropertyType.h"
#include "writers/GraphWriter.h"

#include "ExitWithTestOutcome.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr unsigned TIMEOUT_SECONDS = 60;
constexpr uint64_t UNCREATED_NODE_OFFSET = 1000;

void submitAPropertyOfANodeTheWriterNeverCreated() {
    JobSystem jobSystem;
    jobSystem.init();

    const std::unique_ptr<Graph> graph = Graph::create();

    {
        GraphWriter writer(graph.get(), &jobSystem);

        const NodeID node = writer.addNode({"Person"});
        const NodeID uncreated {node.getValue() + UNCREATED_NODE_OFFSET};
        writer.addNodeProperty<types::Int64>(node, "age", 30);
        writer.addNodeProperty<types::Int64>(uncreated, "age", 31);

        EXPECT_THROW(writer.submit(), std::out_of_range);
    }

    jobSystem.terminate();
}

}

class DataPartLoadJobExceptionTest : public TuringTest {
protected:
    void initialize() override {}

    void terminate() override {}
};

TEST_F(DataPartLoadJobExceptionTest, aThrowInALoadJobReachesTheSubmitter) {
    EXPECT_EXIT(exitWithTestOutcome(submitAPropertyOfANodeTheWriterNeverCreated, TIMEOUT_SECONDS), testing::ExitedWithCode(EXIT_SUCCESS), "");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        GTEST_FLAG_SET(death_test_style, "threadsafe");
    });
}
