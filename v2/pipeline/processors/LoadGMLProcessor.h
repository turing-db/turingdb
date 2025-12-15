#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineValueOutputInterface.h"

namespace db::v2 {

class LoadGMLProcessor : public Processor {
public:
    static LoadGMLProcessor* create(PipelineV2* pipeline,
                                    std::string_view graphName,
                                    std::string_view filePath);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _graphName;
    std::string_view _filePath;
    PipelineValueOutputInterface _outName;

    LoadGMLProcessor(std::string_view graphName,
                     std::string_view filePath);
    ~LoadGMLProcessor();
};

}
