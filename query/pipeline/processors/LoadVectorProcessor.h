#pragma once

#include "Processor.h"

#include <string_view>

#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class LoadVectorProcessor : public Processor {
public:
    static LoadVectorProcessor* create(PipelineV2* pipeline,
                                       std::string_view filePath,
                                       std::string_view indexName);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outCount; }

protected:
    std::string_view _filePath;
    std::string_view _indexName;
    PipelineValueOutputInterface _outCount;

    LoadVectorProcessor(std::string_view filePath, std::string_view indexName);
    ~LoadVectorProcessor();
};

}
