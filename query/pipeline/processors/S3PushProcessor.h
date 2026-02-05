#pragma once

#include <string_view>

#include "Processor.h"
#include "interfaces/PipelineValueOutputInterface.h"
#include "MinioS3ClientWrapper.h"
#include "TuringS3Client.h"

namespace db {

class S3PushProcessor final : public Processor {
public:
    static S3PushProcessor* create(PipelineV2* pipeline,
                                   std::string_view s3Bucket,
                                   std::string_view s3Prefix,
                                   std::string_view s3File,
                                   std::string_view localPath);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValueOutputInterface& output() { return _output; }

protected:
    std::string_view _s3Bucket;
    std::string_view _s3Prefix;
    std::string_view _s3File;
    std::string_view _localPath;

    S3::TuringS3Client<S3::MinioS3ClientWrapper>* _s3Client {nullptr};
    PipelineValueOutputInterface _output;

    S3PushProcessor(std::string_view s3Bucket,
                    std::string_view s3Prefix,
                    std::string_view s3File,
                    std::string_view localPath);
    ~S3PushProcessor();
};

}
