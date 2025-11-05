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
    NamedColumn* addColumnToOutput(ColumnTag tag) {
        return allocColumn<ColumnType>(_pendingOutput->getDataframe(), tag);
    }

    PipelineOutputInterface& addScanNodes();
    PipelineOutputInterface& addGetOutEdges();
    void addLambda(const LambdaProcessor::Callback& callback);

    template <SupportedType T>
    PipelineOutputInterface& addGetNodeProperties(PropertyType propertyType);

    PipelineOutputInterface& addSkip(size_t count);
    PipelineOutputInterface& addLimit(size_t count);
    PipelineOutputInterface& addCount();

    PipelineOutputInterface& addLambdaSource(const LambdaSourceProcessor::Callback& callback);

    PipelineOutputInterface& addMaterialize();

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

    void openMaterialize();
    void closeMaterialize();
};

}
