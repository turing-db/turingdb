#pragma once

#include <string_view>

#include "ChangeOp.h"
#include "EntityType.h"
#include "Path.h"
#include "PipelineV2.h"
#include "PendingOutputView.h"
#include "procedures/ProcedureBlueprint.h"

#include "ProjectionItem.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"

#include "interfaces/PipelineBlockOutputInterface.h"
#include "interfaces/PipelineNodeOutputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"
#include "interfaces/PipelineValueOutputInterface.h"

#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "processors/LambdaTransformProcessor.h"
#include "processors/WriteProcessor.h"

#include "metadata/SupportedType.h"
#include "metadata/LabelSet.h"

#include "LocalMemory.h"
#include "Path.h"

namespace db::v2 {

class PipelineV2;
class PipelineOutputInterface;
class MaterializeProcessor;


class PipelineBuilder {
public:
    using ForkOutputs = std::vector<PipelineBlockOutputInterface>;

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
    PipelineNodeOutputInterface& addScanNodesByLabel(const LabelSet* labelset);
    PipelineBlockOutputInterface& addLambdaSource(const LambdaSourceProcessor::Callback& callback);
    PipelineBlockOutputInterface& addDatabaseProcedure(const ProcedureBlueprint& blueprint,
                                                       std::span<ProcedureBlueprint::YieldItem> yield);
    PipelineBlockOutputInterface& addChangeOp(ChangeOp op);
    PipelineBlockOutputInterface& addCommit();

    // Get LabelSetID
    PipelineValuesOutputInterface& addGetLabelSetID();

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
    PipelineBlockOutputInterface& addHashJoin(PipelineOutputInterface* rhs,
                                              ColumnTag leftJoinKey,
                                              ColumnTag rightJoinKey);

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
    ForkOutputs& addFork(size_t count);

    // Materialize
    PipelineBlockOutputInterface& addMaterialize();
    void setMaterializeProc(MaterializeProcessor* matProc) { _matProc = matProc; }
    MaterializeProcessor* getMaterializeProc() { return _matProc; }
    bool isSingleMaterializeStep() const;

    // Rename output
    void rename(std::string_view name) { _pendingOutput.rename(name); }

    // Write/Updates
    PipelineBlockOutputInterface& addWrite(const WriteProcessor::DeletedNodes& nodeColumnsToDelete,
                                           const WriteProcessor::DeletedEdges& edgeColumnsToDelete,
                                           WriteProcessor::PendingNodes& pendingNodes,
                                           WriteProcessor::PendingEdges& pendingEdges);

    // Load graph
    PipelineValueOutputInterface& addLoadGraph(std::string_view graphName);
    PipelineValueOutputInterface& addLoadGML(std::string_view graphName, const fs::Path& filePath);
    PipelineValueOutputInterface& addLoadNeo4j(std::string_view graphName, const fs::Path& filePath);

    // List Graph
    PipelineValueOutputInterface& addListGraph();

    // Create Graph
    PipelineValueOutputInterface& addCreateGraph(std::string_view graphName);

    // S3 Commands
    void addS3Connect(std::string_view accessId,
                      std::string_view secretKey,
                      std::string_view region);
    void addS3Pull(std::string_view s3Bucket,
                   std::string_view s3Prefix,
                   std::string_view s3File,
                   std::string_view localPath);
    void addS3Push(std::string_view s3Bucket,
                   std::string_view s3Prefix,
                   std::string_view s3File,
                   std::string_view localPath);

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
