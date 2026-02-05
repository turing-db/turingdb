#pragma once

#include <string>
#include <vector>

#include "S3ClientResult.h"

class RemoteStorageClient;
namespace S3 {

template <typename ClientType>
class TuringS3Client {
public:
    explicit TuringS3Client(ClientType&& client);
    explicit TuringS3Client(ClientType& client) = delete;
    TuringS3Client& operator=(const ClientType&) = delete;
    TuringS3Client() = delete;

    ~TuringS3Client() = default;

    S3ClientResult<void> listKeys(const std::string& bucketName,
                                  const std::string& prefix,
                                  std::vector<std::string>& keyResults);
    S3ClientResult<void> listFiles(const std::string& bucketName,
                                   const std::string& prefix,
                                   std::vector<std::string>& keyResults);
    S3ClientResult<void> listFolders(const std::string& bucketName,
                                     const std::string& prefix,
                                     std::vector<std::string>& folderResults);

    S3ClientResult<void> uploadFile(const std::string& filePath,
                                    const std::string& bucketName,
                                    const std::string& keyName);
    S3ClientResult<void> downloadFile(const std::string& filePath,
                                      const std::string& bucketName,
                                      const std::string& keyName);

    S3ClientResult<void> uploadDirectory(const std::string& directory,
                                         const std::string& bucketName,
                                         const std::string& prefix);
    S3ClientResult<void> downloadDirectory(const std::string& directory,
                                           const std::string& bucketName,
                                           const std::string& prefix);

    S3ClientResult<bool> bucketExists(const std::string& bucketName);
    S3ClientResult<void> createDirectoryMarker(const std::string& bucketName,
                                               const std::string& prefix);

private:
    ClientType _client;
};
}
