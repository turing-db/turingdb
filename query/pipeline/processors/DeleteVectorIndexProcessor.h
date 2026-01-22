#pragma once

#include "Processor.h"

#include <string_view>

#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class DeleteVectorIndexProcessor : public Processor {
public:
    static DeleteVectorIndexProcessor* create(PipelineV2* pipeline,
                                              std::string_view indexName);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _indexName;
    PipelineValueOutputInterface _outName;

    DeleteVectorIndexProcessor(std::string_view indexName);
    ~DeleteVectorIndexProcessor();
};

}
