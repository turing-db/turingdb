#pragma once

#include <string>

#include "AwsS3ClientWrapper.h"
#include "TuringS3Client.h"
#include "S3TransferDirectories.h"

namespace db {

class ExecutionContext;
class SystemManager;

class S3PullStep {
public:
    struct Tag {};

    S3PullStep(std::string_view s3Bucket, std::string_view s3Prefix, std::string_view s3File, const std::string& localPath, S3TransferDirectory dir);
    ~S3PullStep();

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    std::string_view _s3Bucket;
    std::string_view _s3Prefix;
    std::string_view _s3File;

    S3TransferDirectory _transferDirectory;
    const std::string& _localPath;
    std::string _fullPath;

    SystemManager* _sysMan {nullptr};

    S3::TuringS3Client<S3::AwsS3ClientWrapper<>>* _s3Client {nullptr};
};

}
