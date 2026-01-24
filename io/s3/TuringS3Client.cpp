#include "TuringS3Client.h"

#include <filesystem>
#include <fstream>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/S3CrtErrors.h>
#include <aws/s3-crt/ClientConfiguration.h>
#include <aws/s3-crt/model/CreateBucketRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/DeleteBucketRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/core/platform/FileSystem.h>

#include "AwsS3ClientWrapper.h"
#include "spdlog/spdlog.h"

using namespace S3;

template <typename ClientType>
TuringS3Client<ClientType>::TuringS3Client(ClientType&& client)
    : _client(std::move(client))
{
}

template <typename T>
S3ClientResult<void> TuringS3Client<T>::listKeys(const std::string& bucketName,
                                                 const std::string& prefix,
                                                 std::vector<std::string>& keyResults) {
    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucketName).WithPrefix(prefix);

    Aws::String continuationToken; // Used for pagination.
    Aws::Vector<Aws::S3Crt::Model::Object> allObjects;

    do {
        if (!continuationToken.empty()) {
            request.SetContinuationToken(continuationToken);
        }

        const auto outcome = _client.ListObjectsV2(request);

        if (!outcome.IsSuccess()) {
            switch (outcome.GetError().GetErrorType()) {
                case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                case Aws::S3Crt::S3CrtErrors::SIGNATURE_DOES_NOT_MATCH:
                case Aws::S3Crt::S3CrtErrors::INVALID_ACCESS_KEY_ID:
                    return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
                case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                    return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
                default:
                    return S3ClientError::result(S3ClientErrorType::CANNOT_LIST_KEYS);
            }
        } else {
            const Aws::Vector<Aws::S3Crt::Model::Object> objects =
                outcome.GetResult().GetContents();

            allObjects.insert(allObjects.end(), objects.begin(), objects.end());
            continuationToken = outcome.GetResult().GetNextContinuationToken();
        }
    } while (!continuationToken.empty());

    for (const auto& object : allObjects) {
        keyResults.push_back(object.GetKey());
    }

    return {};
}

std::string extractFileNameFromKey(const Aws::String& commonPrefix) {
    const size_t lastSlash = commonPrefix.find_last_of('/');

    return commonPrefix.substr(lastSlash + 1);
}

template <typename T>
S3ClientResult<void> TuringS3Client<T>::listFiles(const std::string& bucketName,
                                                  const std::string& prefix,
                                                  std::vector<std::string>& keyResults) {
    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucketName).WithPrefix(prefix).WithDelimiter("/");

    Aws::String continuationToken; // Used for pagination.
    Aws::Vector<Aws::S3Crt::Model::Object> allObjects;

    do {
        if (!continuationToken.empty()) {
            request.SetContinuationToken(continuationToken);
        }

        const auto outcome = _client.ListObjectsV2(request);

        if (!outcome.IsSuccess()) {
            switch (outcome.GetError().GetErrorType()) {
                case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                case Aws::S3Crt::S3CrtErrors::SIGNATURE_DOES_NOT_MATCH:
                case Aws::S3Crt::S3CrtErrors::INVALID_ACCESS_KEY_ID:
                    return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
                case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                    return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
                default:
                    return S3ClientError::result(S3ClientErrorType::CANNOT_LIST_FILES);
            }
        } else {
            const Aws::Vector<Aws::S3Crt::Model::Object> objects =
                outcome.GetResult().GetContents();

            allObjects.insert(allObjects.end(), objects.begin(), objects.end());
            continuationToken = outcome.GetResult().GetNextContinuationToken();
        }
    } while (!continuationToken.empty());

    for (const auto& object : allObjects) {
        keyResults.push_back(extractFileNameFromKey(object.GetKey()));
    }

    return {};
}

std::string commonPrefixToFolderName(const Aws::String& commonPrefix) {
    const size_t lastSlash = commonPrefix.find_last_of('/');
    const size_t secondLastSlash = commonPrefix.find_last_of('/', lastSlash - 1);

    return commonPrefix.substr(secondLastSlash + 1, lastSlash - secondLastSlash - 1);
}

template <typename T>
S3ClientResult<void> TuringS3Client<T>::listFolders(const std::string& bucketName,
                                                    const std::string& prefix,
                                                    std::vector<std::string>& folderResults) {
    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucketName).WithPrefix(prefix).WithDelimiter("/");

    Aws::String continuationToken; // Used for pagination.
    Aws::Vector<Aws::S3Crt::Model::CommonPrefix> allFolders;

    do {
        if (!continuationToken.empty()) {
            request.SetContinuationToken(continuationToken);
        }

        const auto outcome = _client.ListObjectsV2(request);

        if (!outcome.IsSuccess()) {
            switch (outcome.GetError().GetErrorType()) {
                case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                case Aws::S3Crt::S3CrtErrors::SIGNATURE_DOES_NOT_MATCH:
                case Aws::S3Crt::S3CrtErrors::INVALID_ACCESS_KEY_ID:
                    return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
                case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                    return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
                default:
                    return S3ClientError::result(S3ClientErrorType::CANNOT_LIST_FOLDERS);
            }
        } else {
            const Aws::Vector<Aws::S3Crt::Model::CommonPrefix> folders =
                outcome.GetResult().GetCommonPrefixes();

            allFolders.insert(allFolders.end(), folders.begin(), folders.end());
            continuationToken = outcome.GetResult().GetNextContinuationToken();
        }
    } while (!continuationToken.empty());

    for (const auto& folder : allFolders) {
        folderResults.push_back(commonPrefixToFolderName(folder.GetPrefix()));
    }

    return {};
}

template <typename T>
S3ClientResult<void> TuringS3Client<T>::uploadFile(const Aws::String& filePath,
                                                   const Aws::String& bucketName,
                                                   const Aws::String& keyName) {
    std::error_code ec;
    if (!std::filesystem::exists(filePath, ec)) {
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }
        return S3ClientError::result(S3ClientErrorType::FILE_NOT_FOUND);
    }

    Aws::S3Crt::Model::PutObjectRequest request;
    // SetIfNoneMatch("*") makes the upload fail if the resource exists in the bucket
    request.WithBucket(bucketName).WithKey(keyName).SetIfNoneMatch("*");
    std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::FStream>("TuringS3Client", filePath.c_str(), std::ios_base::in | std::ios_base::binary);

    if (!*inputData) {
        return S3ClientError::result(S3ClientErrorType::CANNOT_OPEN_FILE);
    }

    request.SetBody(inputData);

    Aws::S3Crt::Model::PutObjectOutcome outcome = _client.PutObject(request);

    if (!outcome.IsSuccess()) {
        switch (outcome.GetError().GetErrorType()) {
            case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
            case Aws::S3Crt::S3CrtErrors::SIGNATURE_DOES_NOT_MATCH:
            case Aws::S3Crt::S3CrtErrors::INVALID_ACCESS_KEY_ID:
                return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
            case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
            default:
                if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
                    return S3ClientError::result(S3ClientErrorType::FILE_EXISTS);
                }
                return S3ClientError::result(S3ClientErrorType::CANNOT_UPLOAD_FILE);
        }
    }

    return {};
}

template <typename T>
S3ClientResult<void> TuringS3Client<T>::downloadFile(const Aws::String& fileName,
                                                     const Aws::String& bucketName,
                                                     const Aws::String& keyName) {
    Aws::S3Crt::Model::GetObjectRequest request;
    request.WithBucket(bucketName).WithKey(keyName);

    Aws::S3Crt::Model::GetObjectOutcome outcome = _client.GetObject(request);

    if (!outcome.IsSuccess()) {
        switch (outcome.GetError().GetErrorType()) {
            case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
            case Aws::S3Crt::S3CrtErrors::SIGNATURE_DOES_NOT_MATCH:
            case Aws::S3Crt::S3CrtErrors::INVALID_ACCESS_KEY_ID:
                return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
            case Aws::S3Crt::S3CrtErrors::NO_SUCH_KEY:
                return S3ClientError::result(S3ClientErrorType::INVALID_KEY_NAME);
            case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
            default:
                return S3ClientError::result(S3ClientErrorType::CANNOT_DOWNLOAD_FILE);
        }
    }

    std::ofstream outputFileStream(fileName.c_str(), std::ios_base::out | std::ios_base::binary);

    if (!outputFileStream.is_open()) {
        std::error_code ec;
        std::filesystem::remove(fileName, ec);
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }
        return S3ClientError::result(S3ClientErrorType::CANNOT_OPEN_FILE);
    }
    outputFileStream << outcome.GetResult().GetBody().rdbuf();
    outputFileStream.flush();
    outputFileStream.close();

    return {};
}

// Need trailing forward slash at the end of the prefix parameter!
template <typename T>
S3ClientResult<void> TuringS3Client<T>::uploadDirectory(const std::string& directory,
                                                        const std::string& bucketName,
                                                        const std::string& prefix) {
    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    std::error_code ec;
    if (!std::filesystem::exists(directory, ec)) {
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }
        return S3ClientError::result(S3ClientErrorType::DIRECTORY_NOT_FOUND);
    }

    S3ClientResult<void> directoryRes;

    auto visitor = [&prefix, &bucketName, this, &directoryRes](const Aws::FileSystem::DirectoryTree*, const Aws::FileSystem::DirectoryEntry& entry) {
        if (entry && entry.fileType == Aws::FileSystem::FileType::File) {
            // Change to fmt
            Aws::StringStream ssKey;
            const Aws::String relativePath = entry.relativePath;

            ssKey << prefix << relativePath;
            Aws::String keyName = ssKey.str();
            if (auto res = uploadFile(entry.path, bucketName, keyName); !res) {
                directoryRes = res;
                return false;
            }
        }

        return true;
    };

    Aws::FileSystem::DirectoryTree dir(directory);
    dir.TraverseDepthFirst(visitor);
    if (!directoryRes) {
        switch (directoryRes.error().getType()) {
            case S3ClientErrorType::CANNOT_UPLOAD_FILE:
            case S3ClientErrorType::CANNOT_OPEN_FILE:
            case S3ClientErrorType::INVALID_KEY_NAME:
                return S3ClientError::result(S3ClientErrorType::CANNOT_UPLOAD_DIRECTORY);
            case S3ClientErrorType::FILE_EXISTS:
                return S3ClientError::result(S3ClientErrorType::DIRECTORY_EXISTS);
            default:
                return directoryRes;
        }
    }

    return {};
}

bool isFolder(const std::string& path) {
    return (path.find_last_of('/') == path.size() - 1);
}

template <typename T>
S3ClientResult<void> TuringS3Client<T>::downloadDirectory(const std::string& directory,
                                                          const std::string& bucketName,
                                                          const std::string& prefix) {
    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    std::error_code ec;
    std::filesystem::create_directory(directory, ec);
    if (ec) {
        return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
    }

    Aws::String downloadPath;
    std::vector<std::string> results;

    if (auto res = listKeys(bucketName, prefix, results); !res) {
        if (res.error().getType() == S3ClientErrorType::CANNOT_LIST_KEYS) {
            return S3ClientError::result(S3ClientErrorType::CANNOT_DOWNLOAD_DIRECTORY);
        } else {
            return res;
        }
    }

    if (results.empty()) {
        return S3ClientError::result(S3ClientErrorType::INVALID_DIRECTORY_NAME);
    }

    for (const auto& result : results) {
        if (!isFolder(result)) {
            const auto pos = result.find(prefix) + prefix.size();
            downloadPath = fmt::format("{}/{}", directory, result.substr(pos));

            auto lastDelimiter = downloadPath.find_last_of('/');
            std::filesystem::create_directories(downloadPath.substr(0, lastDelimiter).c_str(), ec); // Add error handling
            if (ec) {
                return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
            }
            if (auto res = downloadFile(downloadPath, bucketName, result); !res) {
                switch (res.error().getType()) {
                    case S3ClientErrorType::CANNOT_DOWNLOAD_FILE:
                    case S3ClientErrorType::CANNOT_OPEN_FILE:
                    case S3ClientErrorType::INVALID_KEY_NAME:
                        return S3ClientError::result(S3ClientErrorType::CANNOT_DOWNLOAD_DIRECTORY);
                    default:
                        return res;
                }
            }
        }
    }

    return {};
}

template class S3::TuringS3Client<AwsS3ClientWrapper<>>;
template class S3::TuringS3Client<AwsS3ClientWrapper<MockS3Client>>;
