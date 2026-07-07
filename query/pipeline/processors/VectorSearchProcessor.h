#pragma once

#include "Processor.h"

#include <stdint.h>
#include <string_view>
#include <vector>

#include "interfaces/PipelineValuesOutputInterface.h"

namespace db {

class VectorSearchProcessor : public Processor {
public:
    static VectorSearchProcessor* create(PipelineV2* pipeline,
                                         std::string_view indexName,
                                         uint64_t k,
                                         const std::vector<float>& queryVector);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValuesOutputInterface& outIds() { return _outIds; }

    void setScoreColumn(NamedColumn* scoreColumn) { _scoreColumn = scoreColumn; }

protected:
    std::string_view _indexName;
    uint64_t _k {10};
    std::vector<float> _queryVector;
    PipelineValuesOutputInterface _outIds;
    NamedColumn* _scoreColumn {nullptr};

    VectorSearchProcessor(std::string_view indexName,
                          uint64_t k,
                          const std::vector<float>& queryVector);
    ~VectorSearchProcessor();
};

}
