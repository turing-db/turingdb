#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineValueOutputInterface.h"

#include "Path.h"

namespace db {

class LoadParquetProcessor final : public Processor {
public:
    static LoadParquetProcessor* create(PipelineV2* pipeline,
                                        std::string_view graphName,
                                        const fs::Path& filePath);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _graphName;
    fs::Path _filePath;
    PipelineValueOutputInterface _outName;

    LoadParquetProcessor(std::string_view graphName,
                         const fs::Path& filePath);
    ~LoadParquetProcessor();
};

}
