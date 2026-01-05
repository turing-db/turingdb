#pragma once

#include <string_view>

#include "Processor.h"
#include "interfaces/PipelineValueOutputInterface.h"

namespace db::v2 {

class S3ConnectProcessor final : public Processor {
public:
    static S3ConnectProcessor* create(PipelineV2* pipeline,
                                      std::string_view accessId,
                                      std::string_view secretKey,
                                      std::string_view region);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValueOutputInterface& output() { return _output; }

protected:
    std::string_view _accessId;
    std::string_view _secretKey;
    std::string_view _region;
    PipelineValueOutputInterface _output;

    S3ConnectProcessor(std::string_view accessId,
                       std::string_view secretKey,
                       std::string_view region);
    ~S3ConnectProcessor();
};

}
