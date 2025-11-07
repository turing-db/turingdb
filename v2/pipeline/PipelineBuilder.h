#pragma once

#include "PipelineV2.h"
#include "PipelineInterface.h"

#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
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
                    PipelineV2* pipeline)
        : _mem(mem),
        _pipeline(pipeline),
        _dfMan(pipeline->getDataframeManager())
    {
    }

    PipelineOutputInterface* getOutput() const { return _pendingOutput; }

    // Sources
    PipelineNodeOutputInterface& addScanNodes();

    // Get edges
    PipelineEdgeOutputInterface& addGetOutEdges();
    PipelineEdgeOutputInterface& addGetInEdges();
    PipelineEdgeOutputInterface& addGetEdges();
    void addLambda(const LambdaProcessor::Callback& callback);

    // Properties
    template <SupportedType T>
    PipelineValuesOutputInterface& addGetNodeProperties(PropertyType propertyType);

    // Aggregations
    PipelineBlockOutputInterface& addSkip(size_t count);
    PipelineBlockOutputInterface& addLimit(size_t count);
    PipelineValueOutputInterface& addCount();

    PipelineBlockOutputInterface& addLambdaSource(const LambdaSourceProcessor::Callback& callback);

    PipelineBlockOutputInterface& addMaterialize();

    // Helper to add a column of a given type to the current output dataframe
    template <typename ColumnType>
    NamedColumn* addColumnToOutput(ColumnTag tag) {
        return allocColumn<ColumnType>(_pendingOutput->getDataframe(), tag);
    }

private:
    LocalMemory* _mem {nullptr};
    PipelineV2* _pipeline {nullptr};
    DataframeManager* _dfMan {nullptr};
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
        return allocColumn<ColumnType>(df, _dfMan->allocTag());
    }

    void openMaterialize();
    void closeMaterialize();
};

}
