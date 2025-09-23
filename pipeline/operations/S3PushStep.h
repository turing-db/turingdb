#pragma once

#include <string>

#include "AwsS3ClientWrapper.h"
#include "TuringS3Client.h"

namespace db {

class ExecutionContext;
class SystemManager;
class TuringS3Client;
class JobSystem;

class S3PushStep {
public:
    struct Tag {};

    S3PushStep(std::string_view s3Bucket, std::string_view s3Prefix, std::string_view s3File, const std::string& localPath);
    ~S3PushStep();

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    std::string_view _s3Bucket;
    std::string_view _s3Prefix;
    std::string_view _s3File;

    const std::string& _localPath;

    SystemManager* _sysMan {nullptr};
    S3::TuringS3Client<S3::AwsS3ClientWrapper<>>* _s3Client {nullptr};
};

}
