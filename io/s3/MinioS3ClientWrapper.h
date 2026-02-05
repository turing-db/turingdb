#pragma once

#include <memory>
#include <string>

#include <miniocpp/client.h>
#include <miniocpp/providers.h>

namespace S3 {

class MinioS3ClientWrapper {
public:
    MinioS3ClientWrapper();
    MinioS3ClientWrapper(const std::string& accessKey,
                         const std::string& secretKey,
                         const std::string& region);

    MinioS3ClientWrapper(MinioS3ClientWrapper&&) = default;
    MinioS3ClientWrapper& operator=(MinioS3ClientWrapper&&) = default;

    MinioS3ClientWrapper(MinioS3ClientWrapper&) = delete;
    MinioS3ClientWrapper& operator=(MinioS3ClientWrapper&) = delete;

    minio::s3::Client& client() { return *_client; }

private:
    std::unique_ptr<minio::s3::BaseUrl> _baseUrl;
    std::unique_ptr<minio::creds::Provider> _envProvider;
    std::unique_ptr<minio::creds::Provider> _configProvider;
    std::unique_ptr<minio::creds::Provider> _provider;
    std::unique_ptr<minio::s3::Client> _client;
};

}
