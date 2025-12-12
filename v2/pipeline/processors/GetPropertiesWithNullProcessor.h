#pragma once

#include <memory>

#include "EntityType.h"
#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"

#include "ExecutionContext.h"

#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "iterators/GetPropertiesIterator.h"

namespace db::v2 {

class PipelineV2;

template <EntityType Entity, SupportedType T>
class GetPropertiesWithNullProcessor : public Processor {
public:
    using ChunkWriter = std::conditional_t<Entity == EntityType::Node,
          GetNodePropertiesWithNullChunkWriter<T>,
          GetEdgePropertiesWithNullChunkWriter<T>>;
    using ColumnValues = typename ChunkWriter::ColumnValues;
    using ColumnIDs = typename ChunkWriter::ColumnIDs;

    static GetPropertiesWithNullProcessor* create(PipelineV2* pipeline,
                                                  ColumnTag entityTag,
                                                  PropertyType propType);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineValuesOutputInterface& output() { return _output; }

protected:
    PropertyType _propType;
    ColumnTag _entityTag;
    std::unique_ptr<ChunkWriter> _propWriter;
    PipelineBlockInputInterface _input;
    PipelineValuesOutputInterface _output;

    explicit GetPropertiesWithNullProcessor(ColumnTag entityTag,
                                            PropertyType propType);
    ~GetPropertiesWithNullProcessor() = default;
};

}
