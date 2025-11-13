#include "processors/ProcessorTester.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class ProjectionProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(ProjectionProcessorTest, test) {
    auto [transaction, view, reader] = readGraph();

    LineContainer<NodeID> expLines;
    LineContainer<NodeID> resLines;
    const Tombstones& tombstones = view.tombstones();

    for (const NodeID originID : reader.scanNodes()) {
        ColumnVector<NodeID> tmpNodeIDs = {originID};

        for (const EdgeRecord& edge : reader.getOutEdges(&tmpNodeIDs)) {
            if (tombstones.contains(edge._edgeID)) {
                continue;
            }

            expLines.add({edge._nodeID});
        }
    }

    fmt::println("- Expected results");
    expLines.print(std::cout);

    // Pipeline definition
    const ColumnTag originIDsTag = _builder->addScanNodes().getNodeIDs()->getTag();

    _builder->addGetOutEdges();

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        // Retrieve the results of the current pipeline iteration (chunk)
        EXPECT_EQ(df->size(), 1);

        const ColumnNodeIDs* originIDs = df->getColumn<ColumnNodeIDs>(ColumnTag {0});

        ASSERT_TRUE(originIDs != nullptr);

        const size_t lineCount = originIDs->size();

        for (size_t i = 0; i < lineCount; i++) {
            resLines.add({originIDs->at(i)});
        }
    };

    std::vector<ColumnTag> tags = {originIDsTag};
    _builder->addMaterialize();
    _builder->addProjection(tags);
    _builder->addLambda(callback);

    fmt::println("\n- Executing pipeline with chunk size 100...");
    EXECUTE(view, 100);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));

    fmt::println("\n- Executing pipeline with chunk size 10...");
    resLines.clear();
    EXECUTE(view, 10);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));

    fmt::println("\n- Executing pipeline with chunk size 2...");
    resLines.clear();
    EXECUTE(view, 2);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));

    fmt::println("\n- Executing pipeline with chunk size 1...");
    resLines.clear();
    EXECUTE(view, 1);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
