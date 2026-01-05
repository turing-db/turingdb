#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineValueOutputInterface.h"

#include "Path.h"

namespace db {

class LoadGMLProcessor : public Processor {
public:
    static LoadGMLProcessor* create(PipelineV2* pipeline,
                                    std::string_view graphName,
                                    const fs::Path& filePath);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _graphName;
    fs::Path _filePath;
    PipelineValueOutputInterface _outName;

    LoadGMLProcessor(std::string_view graphName,
                     const fs::Path& filePath);
    ~LoadGMLProcessor();
};

}
