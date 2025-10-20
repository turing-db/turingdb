#pragma once

#include <memory>

namespace Aws {
class SDKOptions;
}

namespace S3 {

class AWSManager {
public:
    static AWSManager& getInstance() {
        static AWSManager instance; // Only initialised once
        return instance;
    }

    AWSManager(const AWSManager&) = delete;
    AWSManager& operator=(const AWSManager&) = delete;
    AWSManager(AWSManager&&) = delete;
    AWSManager& operator=(AWSManager&&) = delete;

private:
    std::unique_ptr<Aws::SDKOptions> _options;

    AWSManager();
    ~AWSManager();
};

}
