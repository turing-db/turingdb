#pragma once

#include <string_view>

#include "EntityType.h"
#include "PipelineV2.h"
#include "PendingOutputView.h"
#include "procedures/ProcedureBlueprint.h"

#include "ProjectionItem.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"

#include "interfaces/PipelineNodeOutputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"
#include "interfaces/PipelineValueOutputInterface.h"

#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "processors/LambdaTransformProcessor.h"

#include "metadata/SupportedType.h"

#include "LocalMemory.h"

namespace db::v2 {
using ForkOutputs = std::vector<PipelineBlockOutputInterface>&;

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
    PipelineBlockOutputInterface& addDatabaseProcedure(const ProcedureBlueprint& blueprint,
                                                       std::span<ProcedureBlueprint::YieldItem> yield);

    // Get edges
    PipelineEdgeOutputInterface& addGetOutEdges();
    PipelineEdgeOutputInterface& addGetInEdges();
    PipelineEdgeOutputInterface& addGetEdges();

    PipelineOutputInterface& projectEdgesOnOtherIDs() {
        _pendingOutput.projectEdgesOnOtherIDs();
        return *_pendingOutput.getInterface();
    }

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
    PipelineValuesOutputInterface& addGetPropertiesWithNull(ColumnTag entityTag,
                                                            PropertyType propertyType);

    template <SupportedType T>
    PipelineValuesOutputInterface& addGetNodePropertiesWithNull(ColumnTag entityTag,
                                                                PropertyType propertyType) {
        return addGetPropertiesWithNull<EntityType::Node, T>(ColumnTag {entityTag}, propertyType);
    }

    template <SupportedType T>
    PipelineValuesOutputInterface& addGetEdgePropertiesWithNull(ColumnTag entityTag,
                                                                PropertyType propertyType) {
        return addGetPropertiesWithNull<EntityType::Edge, T>(entityTag, propertyType);
    }

    // Joins/Products
    // LHS is implict in @ref _pendingOutput
    PipelineBlockOutputInterface& addCartesianProduct(PipelineOutputInterface* rhs);

    // Aggregations
    PipelineBlockOutputInterface& addSkip(size_t count);
    PipelineBlockOutputInterface& addLimit(size_t count);
    PipelineValueOutputInterface& addCount(ColumnTag colTag = ColumnTag {});
    PipelineBlockOutputInterface& addProjection(std::span<ProjectionItem> items);

    // Lambda transform
    PipelineBlockOutputInterface& addLambdaTransform(const LambdaTransformProcessor::Callback& callback);

    // Lambda sink
    void addLambda(const LambdaProcessor::Callback& callback);

    // Fork
    ForkOutputs addFork(size_t count);
    // Materialize
    PipelineBlockOutputInterface& addMaterialize();
    void setMaterializeProc(MaterializeProcessor* matProc) { _matProc = matProc; }
    MaterializeProcessor* getMaterializeProc() { return _matProc; }
    bool isSingleMaterializeStep() const;

    // Join
    PipelineBlockOutputInterface& addHashJoin(PipelineBlockOutputInterface* rhs,
                                              ColumnTag leftJoinKey,
                                              ColumnTag rightJoinKey);

    // Rename output
    void rename(std::string_view name) { _pendingOutput.rename(name); }

    // Helper to add a column of a given type to the current output dataframe
    template <typename ColumnType>
    NamedColumn* addColumnToOutput(ColumnTag tag) {
        return allocColumn<ColumnType>(_pendingOutput.getDataframe(), tag);
    }

    // Helper to add an existing column to the current output dataframe
    NamedColumn* addColumnToOutput(Column* col) {
        NamedColumn* namedCol = NamedColumn::create(_dfMan, col, _dfMan->allocTag());
        _pendingOutput.getDataframe()->addColumn(namedCol);
        return namedCol;
    }

private:
    LocalMemory* _mem {nullptr};
    PipelineV2* _pipeline {nullptr};
    DataframeManager* _dfMan {nullptr};
    PendingOutputView _pendingOutput;
    MaterializeProcessor* _matProc {nullptr};

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
};

}
