#include "S3PullProcessor.h"

#include "spdlog/spdlog.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"
#include "TuringConfig.h"
#include "TuringS3Client.h"

using namespace db::v2;
using namespace db;

S3PullProcessor::S3PullProcessor(std::string_view s3Bucket,
                                 std::string_view s3Prefix,
                                 std::string_view s3File,
                                 std::string_view localPath)
    : _s3Bucket(s3Bucket),
    _s3Prefix(s3Prefix),
    _s3File(s3File),
    _localPath(localPath)
{
}

S3PullProcessor::~S3PullProcessor() {
}

S3PullProcessor* S3PullProcessor::create(PipelineV2* pipeline,
                                         std::string_view s3Bucket,
                                         std::string_view s3Prefix,
                                         std::string_view s3File,
                                         std::string_view localPath) {
    S3PullProcessor* s3Pull = new S3PullProcessor(s3Bucket, s3Prefix, s3File, localPath);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, s3Pull);
    s3Pull->_output.setPort(output);
    s3Pull->addOutput(output);

    s3Pull->postCreate(pipeline);

    return s3Pull;
}

std::string S3PullProcessor::describe() const {
    return fmt::format("S3PullProcessor @={}", fmt::ptr(this));
}

void S3PullProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    _s3Client = _ctxt->getSystemManager()->getS3Client();
    if (_s3Client == nullptr) {
        throw PipelineException("TuringS3Client has not been created");
    }

    markAsPrepared();
}

void S3PullProcessor::reset() {
}

void S3PullProcessor::execute() {
    const SystemManager* sysMan = _ctxt->getSystemManager();

    const fs::Path& dataDir = sysMan->getConfig()->getDataDir();
    const fs::Path absolute = dataDir / std::string(_localPath);

    if (!absolute.isSubDirectory(dataDir)) {
        throw PipelineException("Invalid file path.");
    }

    const fs::Path parent = absolute.parent();
    parent.mkdir();

    if (!_s3File.empty()) {
        const auto awsRes = _s3Client->downloadFile(absolute.get(),
                                                    std::string(_s3Bucket),
                                                    std::string(_s3File));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    } else {
        const auto awsRes = _s3Client->downloadDirectory(absolute.get(),
                                                         std::string(_s3Bucket),
                                                         std::string(_s3Prefix));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    }

    spdlog::info("Sucesfully Pulled From S3!");

    _output.getPort()->writeData();
    finish();
}
