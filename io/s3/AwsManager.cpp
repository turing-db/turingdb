#include "AwsManager.h"

#include <aws/core/Aws.h>

using namespace S3;

AWSManager::AWSManager()
    :_options(std::make_unique<Aws::SDKOptions>())
{
    _options->loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
    _options->httpOptions.initAndCleanupCurl = true;
    _options->httpOptions.installSigPipeHandler = true; // Let AWS handle SIGPIPE

    Aws::InitAPI(*_options);
}

AWSManager::~AWSManager() {
    Aws::ShutdownAPI(*_options);
}
