#pragma once

#include "Processor.h"
#include "EntityType.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"

#include "ExecutionContext.h"

#include "metadata/PropertyType.h"

namespace db {

class PipelineV2;
class ColumnEmbeddingMany;

template <EntityType Entity>
class GetEmbeddingPropertiesProcessor : public Processor {
public:
    using ColumnValues = ColumnEmbeddingMany;

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
    PropertyType _propType;
    uint32_t _dimension;
    PipelineBlockInputInterface _input;
    PipelineValuesOutputInterface _output;

    explicit GetEmbeddingPropertiesProcessor(PropertyType propType, uint32_t dimension);
    ~GetEmbeddingPropertiesProcessor() = default;
};

}
