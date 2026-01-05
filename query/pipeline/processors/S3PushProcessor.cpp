#include "S3PushProcessor.h"

#include "spdlog/spdlog.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"
#include "TuringConfig.h"

using namespace db;

S3PushProcessor::S3PushProcessor(std::string_view s3Bucket,
                                 std::string_view s3Prefix,
                                 std::string_view s3File,
                                 std::string_view localPath)
    : _s3Bucket(s3Bucket),
    _s3Prefix(s3Prefix),
    _s3File(s3File),
    _localPath(localPath)
{
}

S3PushProcessor::~S3PushProcessor() {
}

S3PushProcessor* S3PushProcessor::create(PipelineV2* pipeline,
                                         std::string_view s3Bucket,
                                         std::string_view s3Prefix,
                                         std::string_view s3File,
                                         std::string_view localPath) {
    S3PushProcessor* s3Push = new S3PushProcessor(s3Bucket, s3Prefix, s3File, localPath);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, s3Push);
    s3Push->_output.setPort(output);
    s3Push->addOutput(output);

    s3Push->postCreate(pipeline);

    return s3Push;
}

std::string S3PushProcessor::describe() const {
    return fmt::format("S3PushProcessor @={}", fmt::ptr(this));
}

void S3PushProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    _s3Client = _ctxt->getSystemManager()->getS3Client();
    if (_s3Client == nullptr) {
        throw PipelineException("TuringS3Client has not been created");
    }

    markAsPrepared();
}

void S3PushProcessor::reset() {
}

void S3PushProcessor::execute() {
    const SystemManager* sysMan = _ctxt->getSystemManager();

    const fs::Path& dataDir = sysMan->getConfig()->getDataDir();

    const fs::Path absolute = dataDir / std::string(_localPath);

    if (!absolute.isSubDirectory(dataDir)) {
        throw PipelineException("Invalid file path.");
    }
    if (!absolute.exists()) {
        throw PipelineException("File does not exist");
    }

    if (!_s3File.empty()) {
        const auto awsRes = _s3Client->uploadFile(absolute.get(),
                                            std::string(_s3Bucket),
                                            std::string(_s3File));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    } else {
        auto awsRes = _s3Client->uploadDirectory(absolute.get(),
                                                 std::string(_s3Bucket),
                                                 std::string(_s3Prefix));
        if (!awsRes) {
            throw PipelineException(awsRes.error().fmtMessage());
        }
    }

    spdlog::info("Sucesfully Pushed To S3!");

    _output.getPort()->writeData();
    finish();
}
