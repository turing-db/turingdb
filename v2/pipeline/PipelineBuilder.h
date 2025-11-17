#pragma once

#include <string_view>

#include "EntityType.h"
#include "PipelineV2.h"
#include "PipelineInterface.h"
#include "PendingOutputView.h"

#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"

#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "processors/LambdaTransformProcessor.h"

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

    const PendingOutputView& getPendingOutput() const { return _pendingOutput; }
    PendingOutputView& getPendingOutput() { return _pendingOutput; }
    PipelineOutputInterface* getPendingOutputInterface() const { return _pendingOutput.getInterface(); }

    // Sources
    PipelineNodeOutputInterface& addScanNodes();
    PipelineBlockOutputInterface& addLambdaSource(const LambdaSourceProcessor::Callback& callback);

    // Get edges
    PipelineEdgeOutputInterface& addGetOutEdges();
    PipelineEdgeOutputInterface& addGetInEdges();
    PipelineEdgeOutputInterface& addGetEdges();
    void projectEdgesOnOtherIDs() { _pendingOutput.projectEdgesOnOtherIDs(); }

    // Properties
    template <EntityType Entity, SupportedType T>
    PipelineValuesOutputInterface& addGetProperties(PropertyType propertyType);

    template <SupportedType T>
    PipelineValuesOutputInterface& addGetNodeProperties(PropertyType propertyType) {
        return addGetProperties<EntityType::Node, T>(propertyType);
    }

    template <SupportedType T>
    PipelineValuesOutputInterface& addGetEdgeProperties(PropertyType propertyType) {
        return addGetProperties<EntityType::Edge, T>(propertyType);
    }

    template <EntityType Entity, SupportedType T>
    PipelineValuesOutputInterface& addGetPropertiesWithNull(PropertyType propertyType);

    template <SupportedType T>
    PipelineValuesOutputInterface& addGetNodePropertiesWithNull(PropertyType propertyType) {
        return addGetPropertiesWithNull<EntityType::Node, T>(propertyType);
    }

    template <SupportedType T>
    PipelineValuesOutputInterface& addGetEdgePropertiesWithNull(PropertyType propertyType) {
        return addGetPropertiesWithNull<EntityType::Edge, T>(propertyType);
    }

    // Joins/Products
    // LHS is implict in @ref _pendingOutput
    PipelineBlockOutputInterface& addCartesianProduct(PipelineOutputInterface* rhs);

    // Aggregations
    PipelineBlockOutputInterface& addSkip(size_t count);
    PipelineBlockOutputInterface& addLimit(size_t count);
    PipelineValueOutputInterface& addCount();
    PipelineBlockOutputInterface& addProjection(std::span<ColumnTag> tags);

    // Lambda transform
    PipelineBlockOutputInterface& addLambdaTransform(const LambdaTransformProcessor::Callback& callback);

    // Lambda sink
    void addLambda(const LambdaProcessor::Callback& callback);

    // Materialize
    PipelineBlockOutputInterface& addMaterialize();
    bool isMaterializeOpen() const { return _matProc != nullptr; }
    bool isSingleMaterializeStep() const;
    void closeMaterialize();

    // Rename output
    void rename(std::string_view name) { _pendingOutput.rename(name); }

    // Helper to add a column of a given type to the current output dataframe
    template <typename ColumnType>
    NamedColumn* addColumnToOutput(ColumnTag tag) {
        return allocColumn<ColumnType>(_pendingOutput.getDataframe(), tag);
    }

private:
    LocalMemory* _mem {nullptr};
    PipelineV2* _pipeline {nullptr};
    DataframeManager* _dfMan {nullptr};
    PendingOutputView _pendingOutput;
    MaterializeProcessor* _matProc {nullptr};
    bool _isMaterializeOpen {false};

    template <typename ColumnType>
    NamedColumn* allocColumn(Dataframe* df, ColumnTag tag) {
        ColumnType* col = _mem->alloc<ColumnType>();
        NamedColumn* newCol = NamedColumn::create(_dfMan, col, tag);
        df->addColumn(newCol);
        return newCol;
    }

    template <typename ColumnType>
    NamedColumn* allocColumn(Dataframe* df) {
        return allocColumn<ColumnType>(df, _dfMan->allocTag());
    }

    void openMaterialize();
};

}
