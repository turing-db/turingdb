#include "S3ConnectStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Profiler.h"
#include "SystemManager.h"

using namespace db;

S3ConnectStep::S3ConnectStep(const std::string& accessId, const std::string& secretKey, const std::string& region)
    : _accessId(accessId),
    _secretKey(secretKey),
    _region(region)
{
}

S3ConnectStep::~S3ConnectStep()
{
}

void S3ConnectStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
}

void S3ConnectStep::execute() {
    Profile profile {"S3ConnectStep::execute"};
    _sysMan->setS3Client(_accessId, _secretKey, _region);
}

void S3ConnectStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "S3ConnectStep";
    descr.assign(ss.str());
}
