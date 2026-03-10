#pragma once

#include <memory>

#include "Processor.h"
#include "EntityType.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"

#include "ExecutionContext.h"

#include "metadata/PropertyType.h"
#include "iterators/GetPropertiesIterator.h"

namespace db {

class PipelineV2;
class ColumnEmbeddingMany;

template <EntityType Entity>
class GetEmbeddingPropertiesProcessor : public Processor {
public:
    using ChunkWriter = std::conditional_t<Entity == EntityType::Node,
                                           GetNodePropertiesChunkWriter<types::Embedding>,
                                           GetEdgePropertiesChunkWriter<types::Embedding>>;

    using ColumnValues = typename ChunkWriter::ColumnValues;
    using ColumnIDs = typename ChunkWriter::ColumnIDs;

    static GetEmbeddingPropertiesProcessor* create(PipelineV2* pipeline,
                                                    PropertyType propType,
                                                    uint32_t dimension);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineValuesOutputInterface& output() { return _output; }

protected:
    PropertyType _propType {{}, ValueType::Invalid};
    uint32_t _dimension {0};
    std::unique_ptr<ChunkWriter> _propWriter;
    PipelineBlockInputInterface _input;
    PipelineValuesOutputInterface _output;

    explicit GetEmbeddingPropertiesProcessor(PropertyType propType, uint32_t dimension);
    ~GetEmbeddingPropertiesProcessor() = default;
};

}
