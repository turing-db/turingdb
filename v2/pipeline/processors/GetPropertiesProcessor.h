#pragma once

#include <memory>

#include "EntityType.h"
#include "Processor.h"

#include "interfaces/PipelineNodeInputInterface.h"
#include "interfaces/PipelineEdgeInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"

#include "ExecutionContext.h"

#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "iterators/GetPropertiesIterator.h"

namespace db::v2 {

class PipelineV2;

template <EntityType Entity, SupportedType T>
class GetPropertiesProcessor : public Processor {
public:
    using ChunkWriter = std::conditional_t<Entity == EntityType::Node,
                                           GetNodePropertiesChunkWriter<T>,
                                           GetEdgePropertiesChunkWriter<T>>;

    using InputInterface = std::conditional_t<Entity == EntityType::Node,
                                              PipelineNodeInputInterface,
                                              PipelineEdgeInputInterface>;

    using ColumnValues = typename ChunkWriter::ColumnValues;
    using ColumnIDs = typename ChunkWriter::ColumnIDs;

    static GetPropertiesProcessor* create(PipelineV2* pipeline, PropertyType propType);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    InputInterface& inIDs() { return _inIDs; }
    PipelineValuesOutputInterface& outValues() { return _outValues; }

protected:
    PropertyType _propType;
    std::unique_ptr<ChunkWriter> _propWriter;
    InputInterface _inIDs;
    PipelineValuesOutputInterface _outValues;

    explicit GetPropertiesProcessor(PropertyType propType);
    ~GetPropertiesProcessor() = default;
};

}
