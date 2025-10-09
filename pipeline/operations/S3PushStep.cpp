#include "S3PushStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "PipelineException.h"
#include "Profiler.h"
#include "SystemManager.h"

using namespace db;

S3PushStep::S3PushStep(std::string_view s3Bucket, std::string_view s3Prefix, std::string_view s3File, const std::string& localPath)
    : _s3Bucket(s3Bucket),
    _s3Prefix(s3Prefix),
    _s3File(s3File),
    _localPath(localPath)
{
}

S3PushStep::~S3PushStep() {
}

void S3PushStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
    _s3Client = _sysMan->getS3Client();
    if (_s3Client == nullptr) {
        throw PipelineException("TuringS3Client has not been created");
    }
}

void S3PushStep::execute() {
    Profile profile {"S3PushStep::execute"};
    if (!_s3File.empty()) {
        auto awsRes = _s3Client->uploadFile(_localPath, std::string(_s3Bucket), std::string(_s3File));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    } else {
        auto awsRes = _s3Client->uploadDirectory(_localPath, std::string(_s3Bucket), std::string(_s3Prefix));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    }
}

void S3PushStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "S3PushStep";
    descr.assign(ss.str());
}
