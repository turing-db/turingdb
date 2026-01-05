#pragma once

#include <memory>

#include "Processor.h"

#include "EntityType.h"

#include "interfaces/PipelineBlockInputInterface.h"
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

    using ColumnValues = typename ChunkWriter::ColumnValues;
    using ColumnIDs = typename ChunkWriter::ColumnIDs;

    static GetPropertiesProcessor* create(PipelineV2* pipeline, PropertyType propType);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineValuesOutputInterface& output() { return _output; }

protected:
    PropertyType _propType;
    std::unique_ptr<ChunkWriter> _propWriter;
    PipelineBlockInputInterface _input;
    PipelineValuesOutputInterface _output;

    explicit GetPropertiesProcessor(PropertyType propType);
    ~GetPropertiesProcessor() = default;
};

}
