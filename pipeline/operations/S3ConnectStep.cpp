#include "S3ConnectStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "AwsS3ClientWrapper.h"


using namespace db;

S3ConnectStep::S3ConnectStep(const std::string& accessId, const std::string& secretKey, const std::string& region)
    : _accessId(accessId),
    _secretKey(secretKey),
    _region(region)
{
}

S3ConnectStep::~S3ConnectStep() {
}

void S3ConnectStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
}

void S3ConnectStep::execute() {
    Profile profile {"S3ConnectStep::execute"};
    auto wrapper = S3::AwsS3ClientWrapper<>(_accessId, _secretKey, _region);
    _sysMan->setS3Client(std::make_unique<S3::TuringS3Client<S3::AwsS3ClientWrapper<>>>(std::move(wrapper)));
}

void S3ConnectStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "S3ConnectStep";
    descr.assign(ss.str());
}
