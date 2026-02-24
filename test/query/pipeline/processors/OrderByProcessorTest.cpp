#include <algorithm>
#include <gtest/gtest.h>

#include "EntityType.h"
#include "LineContainer.h"
#include "ProcessorTester.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "dataframe/NamedColumn.h"
#include "iterators/ChunkConfig.h"
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
    using Rows = LineContainer<std::optional<types::String::Primitive>>;

    const auto runTest = [&](bool asc, size_t chunkSize) {
        SCOPED_TRACE("asc=" + std::to_string(asc) +
                     " chunkSize=" + std::to_string(chunkSize));

        auto [transaction, view, reader] = readGraph();
        const PropertyType nameType = view.metadata().propTypes().get("name").value();

        auto* matProc = MaterializeProcessor::create(&_pipeline, &_env->getMem());
        _builder->setMaterializeProc(matProc);
        const auto& scanNodes = _builder->addScanNodes();
        const ColumnTag nodeTag = scanNodes.getNodeIDs()->getTag();
        const auto& getNames =
            _builder->addGetPropertiesWithNull<EntityType::Node, types::String>(nodeTag,
                                                                                nameType);
        const ColumnTag nameTag = getNames.getValues()->getTag();
        OrderByProcessor::OrderByKeys keys = {
            {._col = nameTag, ._asc = asc}
        };
        const PipelineBlockOutputInterface& orderBy = _builder->addOrderBy(keys);
        ASSERT_EQ(orderBy.getDataframe()->size(), 2);

        // Build expected result by collecting all names
        Rows expected;
        {
            for (const NodeID id : reader.scanNodes()) {
                const ColumnNodeIDs idc {id};
                auto names = reader.getNodePropertiesWithNull<types::String>(nameType._id, &idc);
                auto maybeName = *names.begin();
                ASSERT_TRUE(maybeName.has_value());
                expected.add({*maybeName});
            }
        }

        Rows actual;
        bool executed = false;
        const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                         LambdaProcessor::Operation operation) -> void {
            if (operation == LambdaProcessor::Operation::RESET) {
                return;
            }
            executed = true;

            const NamedColumn* nnames = df->getColumn(nameTag);
            ASSERT_TRUE(nnames);
            const auto* casted =
                nnames->getColumn()->cast<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(casted);
            ASSERT_TRUE(casted->size() <= chunkSize);

            // Ensure the names are sorted
            const auto& data = casted->getRaw();
            if (asc) {
                ASSERT_TRUE(std::ranges::is_sorted(data));
            } else {
                ASSERT_TRUE(std::ranges::is_sorted(data, std::greater<> {}));
            }

            // Record the names to compare with expected
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t row = 0; row < rowCount; row++) {
                actual.add({casted->at(row)});
            }
        };
        _builder->addLambda(VERIFY_CALLBACK);
        EXECUTE(view, chunkSize);
        ASSERT_TRUE(executed);
        EXPECT_TRUE(expected.equals(actual));
    };

    for (const bool asc : {true, false}) {
        for (const size_t chunkSize : {1UL, 3UL, 9UL, 16UL, ChunkConfig::CHUNK_SIZE}) {
            runTest(asc, chunkSize);
        }
    }
}

