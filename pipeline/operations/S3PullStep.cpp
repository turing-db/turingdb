#include "S3PullStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "PipelineException.h"
#include "Profiler.h"
#include "TuringS3Client.h"
#include "SystemManager.h"
#include "spdlog/spdlog.h"

using namespace db;

S3PullStep::S3PullStep(std::string_view s3Bucket, std::string_view s3Prefix, std::string_view s3File, const std::string& localPath)
    : _s3Bucket(s3Bucket),
    _s3Prefix(s3Prefix),
    _s3File(s3File),
    _localPath(localPath)
{
}

S3PullStep::~S3PullStep() {
}

void S3PullStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
    _s3Client = _sysMan->getS3Client();
    if (_s3Client == nullptr) {
        throw PipelineException("TuringS3Client has not been created");
    }
}

void S3PullStep::execute() {
    Profile profile {"S3PullStep::execute"};
    if (!_s3File.empty()) {
        auto awsRes = _s3Client->downloadFile(_localPath, std::string(_s3Bucket), std::string(_s3File));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    } else {
        spdlog::info("Downloading file: {}", _s3Prefix);
        auto awsRes = _s3Client->downloadDirectory(_localPath, std::string(_s3Bucket), std::string(_s3Prefix));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    }
}

void S3PullStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "S3PullStep";
    descr.assign(ss.str());
}
