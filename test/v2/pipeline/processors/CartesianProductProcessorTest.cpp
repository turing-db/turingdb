#include "processors/LambdaProcessor.h"
#include "processors/ProcessorTester.h"

#include "processors/CartesianProductProcessor.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class CartesianProductProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(CartesianProductProcessorTest, scanNodesProduct) {
    auto [transaction, view, reader] = readGraph();

    auto& scanNodes2 = _builder->addScanNodes();
    [[maybe_unused]] auto& scanNodes1 = _builder->addScanNodes();

    // scanNodes1 as implicit LHS
    const auto& cartProd = _builder->addCartesianProduct(&scanNodes2);

    ASSERT_EQ(cartProd.getDataframe()->cols().size(), 2);

    [[maybe_unused]] const ColumnTag lhsNodes = cartProd.getDataframe()->cols().front()->getTag();
    [[maybe_unused]] const ColumnTag rhsNodes = cartProd.getDataframe()->cols().back()->getTag();

    const auto callback = [&](const Dataframe* df,
                              LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        for (NamedColumn* col : df->cols()) {
            const ColumnNodeIDs* nodes = col->as<ColumnNodeIDs>();
            spdlog::info("{}", col->getHeader().getName());
            for (NodeID n : *nodes) {
                spdlog::info("\t{}", n.getValue());
                
            }
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, 100);

}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
