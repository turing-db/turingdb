#include "S3ConnectProcessor.h"

#include "spdlog/spdlog.h"

#include "ExecutionContext.h"
#include "SystemManager.h"

using namespace db::v2;
using namespace db;

S3ConnectProcessor::S3ConnectProcessor(std::string_view accessId,
                                       std::string_view secretKey,
                                       std::string_view region)
    : _accessId(accessId),
    _secretKey(secretKey),
    _region(region)
{
}

S3ConnectProcessor::~S3ConnectProcessor() {
}

S3ConnectProcessor* S3ConnectProcessor::create(PipelineV2* pipeline,
                                               std::string_view accessId,
                                               std::string_view secretKey,
                                               std::string_view region) {
    S3ConnectProcessor* s3Connect = new S3ConnectProcessor(accessId, secretKey, region);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, s3Connect);
    s3Connect->_output.setPort(output);
    s3Connect->addOutput(output);

    s3Connect->postCreate(pipeline);

    return s3Connect;
}

std::string S3ConnectProcessor::describe() const {
    return fmt::format("S3ConnectProcessor @={}", fmt::ptr(this));
}

void S3ConnectProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void S3ConnectProcessor::reset() {
}

void S3ConnectProcessor::execute() {
    SystemManager* sysMan = _ctxt->getSystemManager();

    sysMan->setS3Client(std::string(_accessId),
                        std::string(_secretKey),
                        std::string(_region));

    spdlog::info("Sucesfully Connected To S3 Account!");
    _output.getPort()->writeData();
    finish();
}
