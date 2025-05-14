#pragma once

#include "S3Result.h"
#include <memory>
#include <aws/core/utils/memory/stl/AWSString.h>

class RemoteStorageClient;
namespace S3 {
template <typename T>
class TuringS3Client {
public:
    explicit TuringS3Client(T& client);
    ~TuringS3Client() = default;

    void listBuckets();
    bool createBucket(const std::string& bucketName);
    bool deleteBucket(const std::string& bucketName);

    Result<void> listKeys(const std::string& bucketName, std::vector<std::string>& keyResults, const std::string& prefix);
    Result<void> listFiles(const std::string& bucketName, std::vector<std::string>& keyResults, const std::string& prefix);
    Result<void> listFolders(const std::string& bucketName, std::vector<std::string>& folderResults, const std::string& prefix);

    Result<void> uploadFile(const std::string& filePath, const std::string& bucketName, const std::string& keyName);
    Result<void> downloadFile(const std::string& filePath, const std::string& bucketName, const std::string& keyName);

    Result<void> uploadDirectory(const std::string& directory, const std::string& bucketName, const std::string& prefix);
    Result<void> downloadDirectory(const std::string& directory, const std::string& bucketName, const std::string& prefix);

private:
    T& _client;
};



}
