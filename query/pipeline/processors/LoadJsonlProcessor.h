#pragma once

#include <string_view>

#include "Processor.h"
#include "Path.h"

#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class LoadJsonlProcessor : public Processor {
public:
    static LoadJsonlProcessor* create(PipelineV2* pipeline,
                                      const fs::Path& path,
                                      std::string_view graphName);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    fs::Path _path;
    std::string_view _graphName;
    PipelineValueOutputInterface _outName;

    LoadJsonlProcessor(const fs::Path& path, std::string_view graphName);
    ~LoadJsonlProcessor() override;
};

}
