#include "MinioS3ClientWrapper.h"

#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <list>
#include <string>

using namespace S3;

namespace {

std::string getRegionFromAwsConfig() {
    const char* home = getenv("HOME");
    if (!home) {
        return "";
    }

    std::string configPath = std::string(home) + "/.aws/config";
    std::ifstream configFile(configPath);
    if (!configFile.is_open()) {
        return "";
    }

    std::string line;
    bool inDefaultProfile = false;

    while (std::getline(configFile, line)) {
        // Trim whitespace
        size_t start = line.find_first_not_of(" \t");
        if (start == std::string::npos) {
            continue;
        }
        line = line.substr(start);

        // Check for profile header
        if (line[0] == '[') {
            inDefaultProfile = (line == "[default]");
            continue;
        }

        // Look for region in default profile
        if (inDefaultProfile && line.rfind("region", 0) == 0) {
            size_t eqPos = line.find('=');
            if (eqPos != std::string::npos) {
                std::string value = line.substr(eqPos + 1);
                size_t valueStart = value.find_first_not_of(" \t");
                if (valueStart != std::string::npos) {
                    return value.substr(valueStart);
                }
            }
        }
    }

    return "";
}

}

MinioS3ClientWrapper::MinioS3ClientWrapper() 
{
    std::string endpoint = "s3.amazonaws.com";
    std::string region;

    const char* envRegion = getenv("AWS_REGION");
    if (!envRegion) {
        envRegion = getenv("AWS_DEFAULT_REGION");
    }

    if (envRegion && strlen(envRegion) > 0) {
        region = envRegion;
    } else {
        region = getRegionFromAwsConfig();
    }

    if (!region.empty()) {
        endpoint = "s3." + region + ".amazonaws.com";
    }

    _baseUrl = std::make_unique<minio::s3::BaseUrl>(endpoint);

    _envProvider = std::make_unique<minio::creds::EnvAwsProvider>();

    // AwsConfigProvider has a bug: it uses "/aws/credentials" instead of "/.aws/credentials"
    // So we explicitly pass the correct path
    std::string credentialsPath;
    const char* home = getenv("HOME");
    if (home) {
        credentialsPath = std::string(home) + "/.aws/credentials";
    }
    _configProvider = std::make_unique<minio::creds::AwsConfigProvider>(credentialsPath);

    _provider = std::make_unique<minio::creds::ChainedProvider>(
        std::list<minio::creds::Provider*>{_envProvider.get(), _configProvider.get()});

    _client = std::make_unique<minio::s3::Client>(*_baseUrl, _provider.get());
}

MinioS3ClientWrapper::MinioS3ClientWrapper(const std::string& accessKey,
                                           const std::string& secretKey,
                                           const std::string& region) 
{
    const std::string endpoint = "s3." + region + ".amazonaws.com";
    _baseUrl = std::make_unique<minio::s3::BaseUrl>(endpoint);

    _provider = std::make_unique<minio::creds::StaticProvider>(accessKey, secretKey);

    _client = std::make_unique<minio::s3::Client>(*_baseUrl, _provider.get());
}
