#pragma once

#include "PipelineV2.h"
#include "PipelineInterface.h"

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
                    ColumnTagManager* tagManager)
        : _mem(mem),
        _pipeline(pipeline),
        _dfMan(pipeline->getDataframeManager()),
        _tagManager(tagManager)
    {
    }

    PipelineOutputInterface* getOutput() const { return _pendingOutput; }

    PipelineNodeOutputInterface& addScanNodes();
    PipelineEdgeOutputInterface& addGetOutEdges();
    void addLambda(const LambdaProcessor::Callback& callback);

    template <SupportedType T>
    PipelineValuesOutputInterface& addGetNodeProperties(PropertyType propertyType);

    PipelineBlockOutputInterface& addSkip(size_t count);
    PipelineBlockOutputInterface& addLimit(size_t count);
    PipelineValueOutputInterface& addCount();

    PipelineBlockOutputInterface& addLambdaSource(const LambdaSourceProcessor::Callback& callback);

    PipelineBlockOutputInterface& addMaterialize();

    template <typename ColumnType>
    NamedColumn* addColumnToOutput(ColumnTag tag) {
        return allocColumn<ColumnType>(_pendingOutput->getDataframe(), tag);
    }

private:
    LocalMemory* _mem {nullptr};
    PipelineV2* _pipeline {nullptr};
    DataframeManager* _dfMan {nullptr};
    ColumnTagManager* _tagManager {nullptr};
    PipelineOutputInterface* _pendingOutput {nullptr};
    MaterializeProcessor* _matProc {nullptr};

    template <typename ColumnType>
    NamedColumn* allocColumn(Dataframe* df, ColumnTag tag) {
        ColumnType* col = _mem->alloc<ColumnType>();
        NamedColumn* newCol = NamedColumn::create(_dfMan, col, ColumnHeader(tag));
        df->addColumn(newCol);
        return newCol;
    }

    template <typename ColumnType>
    NamedColumn* allocColumn(Dataframe* df) {
        return allocColumn<ColumnType>(df, _tagManager->allocTag());
    }

    void openMaterialize();
    void closeMaterialize();
};

}
