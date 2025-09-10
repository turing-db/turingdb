#include <aws/core/Aws.h>

class AWSManager {
private:
    Aws::SDKOptions _options;

    AWSManager() {
        _options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
        _options.httpOptions.initAndCleanupCurl = true;
        _options.httpOptions.installSigPipeHandler = true; // Let AWS handle SIGPIPE

        Aws::InitAPI(_options);
    }

    ~AWSManager() {
        Aws::ShutdownAPI(_options);
    }

public:
    static AWSManager& getInstance() {
        static AWSManager instance; // Only initialised once
        return instance;
    }

    AWSManager(const AWSManager&) = delete;
    AWSManager& operator=(const AWSManager&) = delete;
    AWSManager(AWSManager&&) = delete;
    AWSManager& operator=(AWSManager&&) = delete;
};
