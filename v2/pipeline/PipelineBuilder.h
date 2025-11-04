#pragma once

#include "dataframe/ColumnTagManager.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"

#include "metadata/SupportedType.h"

#include "LocalMemory.h"

namespace db::v2 {

class PipelineV2;
class PipelineOutputInterface;
class MaterializeProcessor;

class PipelineBuilder {
public:
    PipelineBuilder(LocalMemory* mem,
                    PipelineV2* pipeline,
                    ColumnTagManager& tagManager)
        : _mem(mem),
        _pipeline(pipeline),
        _tagManager(tagManager)
    {
    }

    PipelineOutputInterface* getOutput() const { return _pendingOutput; }

    template <typename ColumnType>
    PipelineBuilder& addColumnToOutput(ColumnTag tag) {
        allocColumn<ColumnType>(_pendingOutput->getDataframe(), tag);
        return *this;
    }

    PipelineBuilder& addScanNodes();
    PipelineBuilder& addGetOutEdges();
    PipelineBuilder& addLambda(const LambdaProcessor::Callback& callback);

    template <SupportedType T>
    PipelineBuilder& addGetNodeProperties(PropertyType propertyType);

    PipelineBuilder& addSkip(size_t count);
    PipelineBuilder& addLimit(size_t count);
    PipelineBuilder& addCount();

    PipelineBuilder& addLambdaSource(const LambdaSourceProcessor::Callback& callback);

    PipelineBuilder& addMaterialize();

private:
    LocalMemory* _mem {nullptr};
    PipelineV2* _pipeline {nullptr};
    ColumnTagManager& _tagManager;
    PipelineOutputInterface* _pendingOutput {nullptr};
    MaterializeProcessor* _matProc {nullptr};

    template <typename ColumnType>
    NamedColumn* allocColumn(Dataframe* df, ColumnTag tag) {
        ColumnType* col = _mem->alloc<ColumnType>();
        return NamedColumn::create(df, col, ColumnHeader(tag));
    }

    template <typename ColumnType>
    NamedColumn* allocColumn(Dataframe* df) {
        return allocColumn<ColumnType>(df, _tagManager.allocTag());
    }

    PipelineBuilder& openMaterialize();
    PipelineBuilder& closeMaterialize();

    PipelineBuilder& connectToBlockInput(PipelineBlockInputInterface& input);
};

}
