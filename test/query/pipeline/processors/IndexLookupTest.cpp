#include <gtest/gtest.h>

#include <algorithm>

#include "columns/AllowedKinds.h"
#include "processors/ProcessorTester.h"
#include "processors/IndexLookupProcessor.h"
#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "processors/MaterializeProcessor.h"

#include "indexes/PropertyHashIndex.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "EntityOutputStream.h"
#include "SystemManager.h"
#include "SimpleGraph.h"

using namespace db;
using namespace turing::test;

class IndexLookupProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(IndexLookupProcessorTest, intIndexInit) {
    auto [transaction, view, reader] = readGraph();

    const PropertyType agePropType = view.metadata().propTypes().get("age").value();

    PropertyHashIndex<types::Int64, NodeID> ageIndex("age_idx", agePropType._id);
    ageIndex.init(view);

    // Only Remy (0) and Adam (1) have an age property
    EXPECT_EQ(ageIndex.size(), 2u);
}

TEST_F(IndexLookupProcessorTest, stringIndexInit) {
    auto [transaction, view, reader] = readGraph();

    const PropertyType namePropType = view.metadata().propTypes().get("name").value();

    PropertyHashIndex<types::String, NodeID> nameIndex("name_idx", namePropType._id);
    nameIndex.init(view);

    // All 18 nodes in SimpleGraph have a name property
    EXPECT_EQ(nameIndex.size(), 18u);
}

TEST_F(IndexLookupProcessorTest, intIndexQueryByAge) {
    auto [transaction, view, reader] = readGraph();

    const PropertyType agePropType = view.metadata().propTypes().get("age").value();

    PropertyHashIndex<types::Int64, NodeID> ageIndex("age_idx", agePropType._id);
    ageIndex.init(view);

    ColumnVector<int64_t> queryCol;
    queryCol.push_back(32);

    ColumnNodeIDs resultCol;
    ageIndex.query(&queryCol, &resultCol);

    // Remy (0) and Adam (1) both have age=32
    ASSERT_EQ(resultCol.size(), 2u);

    const std::vector<NodeID> results(resultCol.begin(), resultCol.end());
    EXPECT_NE(std::find(results.begin(), results.end(), NodeID(0)), results.end());
    EXPECT_NE(std::find(results.begin(), results.end(), NodeID(1)), results.end());
}

TEST_F(IndexLookupProcessorTest, stringIndexQueryByName) {
    auto [transaction, view, reader] = readGraph();

    const PropertyType namePropType = view.metadata().propTypes().get("name").value();

    PropertyHashIndex<types::String, NodeID> nameIndex("name_idx", namePropType._id);
    nameIndex.init(view);

    ColumnVector<std::string_view> queryCol;
    queryCol.push_back("Adam");

    ColumnNodeIDs resultCol;
    nameIndex.query(&queryCol, &resultCol);

    ASSERT_EQ(resultCol.size(), 1u);
    EXPECT_EQ(resultCol.front(), NodeID(1)); // Adam is NodeID 1
}

TEST_F(IndexLookupProcessorTest, pipelineLambdaSourceThenIntLookup) {
    auto [transaction, view, reader] = readGraph();

    const PropertyType agePropType = view.metadata().propTypes().get("age").value();

    PropertyHashIndex<types::Int64, NodeID> ageIndex("age_idx", agePropType._id);
    ageIndex.init(view);

    // LambdaSource provides Remy's NodeID as the starting node
    const NodeID REMY_ID {0};
    const auto remySource = [&](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation op) {
        if (op != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        ASSERT_EQ(df->size(), 1);
        auto* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        nodeIDs->emplace_back(REMY_ID);
        isFinished = true;
    };

    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));

    const ColumnTag sourceTag = _pipeline.getDataframeManager()->allocTag();
    auto& lambdaOutput = _builder->addLambdaSource(remySource);
    _builder->addColumnToOutput<ColumnNodeIDs>(sourceTag);
    lambdaOutput.setStream(EntityOutputStream::createNodeStream(sourceTag));

    // Extract the age property from the source node
    PipelineValuesOutputInterface& propOutput =
        _builder->addGetNodeProperties<types::Int64>(agePropType);
    const ColumnTag ageTag = propOutput.getValues()->getTag();
    NamedColumn* values = propOutput.getDataframe()->getColumn(ageTag);
    propOutput.setValues(values);

    // Look up all nodes whose age matches the extracted value
    const PipelineValuesOutputInterface& lookupOutput =
        _builder->addIndexLookup<types::Int64::Primitive, NodeID>(&ageIndex);
    
    const ColumnTag resultTag = lookupOutput.getValues()->getTag();

    bool executed = false;
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        const auto* results = df->getColumn<ColumnNodeIDs>(resultTag);
        ASSERT_TRUE(results != nullptr);

        // Remy has age=32, so the index lookup should return all nodes with age=32:
        // Remy (0) and Adam (1)
        ASSERT_EQ(results->size(), 2u);
        const std::vector<NodeID> resVec(results->begin(), results->end());
        EXPECT_NE(std::find(resVec.begin(), resVec.end(), NodeID(0)), resVec.end());
        EXPECT_NE(std::find(resVec.begin(), resVec.end(), NodeID(1)), resVec.end());
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, 1);
    ASSERT_TRUE(executed);
}

TEST_F(IndexLookupProcessorTest, pipelineLambdaSourceThenStringLookup) {
    auto [transaction, view, reader] = readGraph();

    const PropertyType namePropType = view.metadata().propTypes().get("name").value();

    PropertyHashIndex<types::String, NodeID> nameIndex("name_idx", namePropType._id);
    nameIndex.init(view);

    // LambdaSource provides Luc's NodeID (9) as the starting node
    const NodeID LUC_ID {9};
    const auto lucSource = [&](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation op) {
        if (op != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        ASSERT_EQ(df->size(), 1);
        auto* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        nodeIDs->emplace_back(LUC_ID);
        isFinished = true;
    };

    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));

    const ColumnTag sourceTag = _pipeline.getDataframeManager()->allocTag();
    auto& lambdaOutput = _builder->addLambdaSource(lucSource);
    _builder->addColumnToOutput<ColumnNodeIDs>(sourceTag);
    lambdaOutput.setStream(EntityOutputStream::createNodeStream(sourceTag));

    // Extract the name property from the source node
    _builder->addGetNodeProperties<types::String>(namePropType);

    // Look up all nodes whose name matches the extracted value
    const auto& lookupOutput = _builder->addIndexLookup<types::String::Primitive, NodeID>(&nameIndex);
    const ColumnTag resultTag = lookupOutput.getValues()->getTag();

    bool executed = false;
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        const auto* results = df->getColumn<ColumnNodeIDs>(resultTag);
        ASSERT_TRUE(results != nullptr);

        // Luc's name is "Luc", so the index lookup returns only Luc (9)
        ASSERT_EQ(results->size(), 1u);
        EXPECT_EQ(results->front(), NodeID(9));
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, 1);
    ASSERT_TRUE(executed);
}
