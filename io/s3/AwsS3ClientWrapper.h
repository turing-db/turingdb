#pragma once

#include <aws/core/Aws.h>
#include <aws/s3-crt/ClientConfiguration.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/PutObjectRequest.h>

#include "AwsManager.h"
#include "MockS3Client.h"

namespace S3 {

template <typename ClientType = Aws::S3Crt::S3CrtClient>
class AwsS3ClientWrapper {
public:
    AwsS3ClientWrapper() {
        init();
    }

    AwsS3ClientWrapper(const std::string& accessKey, const std::string& secretKey, const std::string& region) {
        init(accessKey, secretKey, region);
    }

    AwsS3ClientWrapper(AwsS3ClientWrapper&&) = default;
    AwsS3ClientWrapper& operator=(AwsS3ClientWrapper&&) = default;

    // Don't allow object to be copied so we don't get
    // into undefined states
    AwsS3ClientWrapper(AwsS3ClientWrapper&) = delete;
    AwsS3ClientWrapper& operator=(AwsS3ClientWrapper&) = delete;

    Aws::S3Crt::Model::PutObjectOutcome PutObject(Aws::S3Crt::Model::PutObjectRequest& request) {
        return _client->PutObject(request);
    }

    Aws::S3Crt::Model::GetObjectOutcome GetObject(Aws::S3Crt::Model::GetObjectRequest& request) {
        return _client->GetObject(request);
    }
    Aws::S3Crt::Model::ListObjectsV2Outcome ListObjectsV2(Aws::S3Crt::Model::ListObjectsV2Request& request) {
        return _client->ListObjectsV2(request);
    }

private:
    void init() {
        AWSManager::getInstance();

        _clientConfig = std::make_unique<Aws::S3Crt::ClientConfiguration>();
        _clientConfig->region = "eu-west-2";

        // All client config options need to changed before here
        _client = Aws::MakeUnique<ClientType>("S3Client", *_clientConfig);
    }

    void init(const std::string& accessKey, const std::string& secretKey, const std::string& region) {
        AWSManager::getInstance();

        _clientConfig = std::make_unique<Aws::S3Crt::ClientConfiguration>();
        _clientConfig->region = region;

        const auto auth = Aws::Auth::AWSCredentials(accessKey, secretKey);

        // All client config options need to changed before here
        _client = Aws::MakeUnique<ClientType>("S3Client", auth, *_clientConfig);
    }

    Aws::UniquePtr<ClientType> _client;
    std::unique_ptr<Aws::S3Crt::ClientConfiguration> _clientConfig;
};

template <>
class AwsS3ClientWrapper<MockS3Client> {
public:
    explicit AwsS3ClientWrapper(MockS3Client& client)
    :_client(client)
    {
    }

    Aws::S3Crt::Model::PutObjectOutcome PutObject(Aws::S3Crt::Model::PutObjectRequest& request) {
        return _client.PutObject(request);
    }

    Aws::S3Crt::Model::GetObjectOutcome GetObject(Aws::S3Crt::Model::GetObjectRequest& request) {
        return _client.GetObject(request);
    }

    Aws::S3Crt::Model::ListObjectsV2Outcome ListObjectsV2(Aws::S3Crt::Model::ListObjectsV2Request& request) {
        return _client.ListObjectsV2(request);
    }

private:
    MockS3Client& _client;
};
}
