#pragma once

#include "Processor.h"

#include <stdint.h>
#include <string_view>

#include "interfaces/PipelineValueOutputInterface.h"
#include "VecLibMetadata.h"

namespace db {

class CreateVectorIndexProcessor : public Processor {
public:
    static CreateVectorIndexProcessor* create(PipelineV2* pipeline,
                                              std::string_view indexName,
                                              vec::Dimension dimension,
                                              vec::DistanceMetric metric);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _indexName;
    vec::Dimension _dimension {0};
    vec::DistanceMetric _metric {vec::DistanceMetric::EUCLIDEAN_DIST};
    PipelineValueOutputInterface _outName;

    CreateVectorIndexProcessor(std::string_view indexName,
                               vec::Dimension dimension,
                               vec::DistanceMetric metric);
    ~CreateVectorIndexProcessor();
};

}
