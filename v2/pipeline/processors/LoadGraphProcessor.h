#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineValueOutputInterface.h"

namespace db::v2 {

class LoadGraphProcessor : public Processor {
public:
    static LoadGraphProcessor* create(PipelineV2* pipeline, std::string_view graphName);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _graphName;
    PipelineValueOutputInterface _outName;

    LoadGraphProcessor(std::string_view graphName);
    ~LoadGraphProcessor();
};

}
