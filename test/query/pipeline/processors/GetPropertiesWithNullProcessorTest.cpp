#include "processors/ProcessorTester.h"

#include "processors/MaterializeProcessor.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace turing::test;

class GetPropertiesWithNullProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(GetPropertiesWithNullProcessorTest, test) {
    auto [transaction, view, reader] = readGraph();

    const PropertyType ageType = view.metadata().propTypes().get("age").value();

    LineContainer<NodeID, std::optional<int64_t>, EdgeID, NodeID, EdgeTypeID> expLines;
    LineContainer<NodeID, std::optional<int64_t>, EdgeID, NodeID, EdgeTypeID> resLines;

    const Tombstones& tombstones = view.tombstones();

    for (const NodeID originID : reader.scanNodes()) {
        ColumnVector<NodeID> srcNodeIDs = {originID};

        auto nameIt = reader.getNodePropertiesWithNull<types::Int64>(ageType._id, &srcNodeIDs).begin();
        for (; nameIt.isValid(); nameIt.next()) {
            ColumnVector<NodeID> filteredNodeIDs = {nameIt.getCurrentID().getValue()};

            for (const EdgeRecord& edge : reader.getOutEdges(&filteredNodeIDs)) {
                if (tombstones.contains(edge._edgeID)) {
                    continue;
                }
                expLines.add({edge._nodeID, nameIt.get(), edge._edgeID, edge._otherID, edge._edgeTypeID});
            }
        }
    }

    fmt::println("- Expected results");
    expLines.print(std::cout);

    // Pipeline definition
    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    const ColumnTag originIDsTag = _builder->addScanNodes().getNodeIDs()->getTag();

    const auto& propInterface = _builder->addGetPropertiesWithNull<EntityType::Node, types::Int64>(originIDsTag, ageType);
    const ColumnTag& agesTag = propInterface.getValues()->getTag();

    _builder->addMaterialize();
    _builder->setMaterializeProc(MaterializeProcessor::createFromPrev(&_pipeline,
                                                                      &_env->getMem(),
                                                                      *_builder->getMaterializeProc()));

    const auto& edgeInterface = _builder->addGetOutEdges();

    const ColumnTag edgeIDsTag = edgeInterface.getEdgeIDs()->getTag();
    const ColumnTag edgeTypesTag = edgeInterface.getEdgeTypes()->getTag();
    const ColumnTag otherIDsTag = edgeInterface.getOtherNodes()->getTag();

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        // Retrieve the results of the current pipeline iteration (chunk)
        EXPECT_EQ(df->size(), 5);

        const ColumnNodeIDs* originIDs = df->getColumn<ColumnNodeIDs>(originIDsTag);
        ASSERT_TRUE(originIDs != nullptr);

        const ColumnOptVector<int64_t>* ages = df->getColumn<ColumnOptVector<int64_t>>(agesTag);
        ASSERT_TRUE(ages != nullptr);

        const ColumnEdgeIDs* edgeIDs = df->getColumn<ColumnEdgeIDs>(edgeIDsTag);
        ASSERT_TRUE(edgeIDs != nullptr);

        const ColumnNodeIDs* otherIDs = df->getColumn<ColumnNodeIDs>(otherIDsTag);
        ASSERT_TRUE(otherIDs != nullptr);

        const ColumnEdgeTypes* edgeTypes = df->getColumn<ColumnEdgeTypes>(edgeTypesTag);
        ASSERT_TRUE(edgeTypes != nullptr);

        const size_t lineCount = originIDs->size();
        ASSERT_EQ(ages->size(), lineCount);
        ASSERT_EQ(edgeIDs->size(), lineCount);
        ASSERT_EQ(otherIDs->size(), lineCount);
        ASSERT_EQ(edgeTypes->size(), lineCount);

        for (size_t i = 0; i < lineCount; i++) {
            resLines.add({originIDs->at(i), ages->at(i), edgeIDs->at(i), otherIDs->at(i), edgeTypes->at(i)});
        }
    };

    _builder->addMaterialize();
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
