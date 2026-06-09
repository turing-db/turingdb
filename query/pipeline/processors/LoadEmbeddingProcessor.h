#pragma once

#include "Processor.h"

#include <string_view>

#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class MetadataBuilder;
class CommitWriteBuffer;

class LoadEmbeddingProcessor : public Processor {
public:
    static LoadEmbeddingProcessor* create(PipelineV2* pipeline,
                                          std::string_view filePath,
                                          std::string_view propertyName);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValueOutputInterface& output() { return _outCount; }

protected:
    std::string_view _filePath;
    std::string_view _propertyName;
    PipelineValueOutputInterface _outCount;

    MetadataBuilder* _metadataBuilder {nullptr};
    CommitWriteBuffer* _writeBuffer {nullptr};

    LoadEmbeddingProcessor(std::string_view filePath, std::string_view propertyName);
    ~LoadEmbeddingProcessor();
};

}
