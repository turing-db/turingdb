#include <gtest/gtest.h>

#include "EntityType.h"
#include "ProcessorTester.h"
#include "SimpleGraph.h"
#include "processors/MaterializeProcessor.h"

#include "processors/OrderByProcessor.h"

class OrderByProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(OrderByProcessorTest, simpleOrder) {
    auto [transaction, view, reader] = readGraph();

    const size_t NUM_NODES_IN_SCAN = reader.getTotalNodesAllocated();
    const PropertyType nameType = view.metadata().propTypes().get("name").value();

    auto* matProc = MaterializeProcessor::create(&_pipeline, &_env->getMem());

    _builder->setMaterializeProc(matProc);

    PipelineNodeOutputInterface& scanNodes = _builder->addScanNodes();
    const ColumnTag nodeTag = scanNodes.getNodeIDs()->getTag();
    PipelineValuesOutputInterface& getNames =
        _builder->addGetPropertiesWithNull<EntityType::Node, types::String>(nodeTag,
                                                                            nameType);
}

