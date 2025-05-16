#include "TuringS3Client.h"

#include "AwsS3ClientWrapper.h"

#include <filesystem>
#include <iostream>
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

#include <spdlog/spdlog.h>

namespace S3 {

template <typename T>
TuringS3Client<T>::TuringS3Client(T& client)
    : _client(client)
{
}

template <typename T>
Result<void> TuringS3Client<T>::listKeys(const std::string& bucketName,
                                         std::vector<std::string>& keyResults,
                                         const std::string& prefix) {
    if (prefix.back() != '/') {
        return Error::result(ErrorType::INVALID_PREFIX_STRING);
    }

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucketName).WithPrefix(prefix);

    Aws::String continuationToken; // Used for pagination.
    Aws::Vector<Aws::S3Crt::Model::Object> allObjects;

    do {
        if (!continuationToken.empty()) {
            request.SetContinuationToken(continuationToken);
        }

        auto outcome = _client.ListObjectsV2(request);

        if (!outcome.IsSuccess()) {
            switch (outcome.GetError().GetErrorType()) {
                case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                    return Error::result(ErrorType::ACCESS_DENIED);
                case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                    return Error::result(ErrorType::INVALID_BUCKET_NAME);
                default:
                    return Error::result(ErrorType::CANNOT_LIST_KEYS);
            }
        } else {
            Aws::Vector<Aws::S3Crt::Model::Object> objects =
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
    size_t lastSlash = commonPrefix.find_last_of('/');

    return commonPrefix.substr(lastSlash + 1);
}

template <typename T>
Result<void> TuringS3Client<T>::listFiles(const std::string& bucketName,
                                          std::vector<std::string>& keyResults,
                                          const std::string& prefix) {
    if (prefix.back() != '/') {
        return Error::result(ErrorType::INVALID_PREFIX_STRING);
    }

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucketName).WithPrefix(prefix).WithDelimiter("/");

    Aws::String continuationToken; // Used for pagination.
    Aws::Vector<Aws::S3Crt::Model::Object> allObjects;

    do {
        if (!continuationToken.empty()) {
            request.SetContinuationToken(continuationToken);
        }

        auto outcome = _client.ListObjectsV2(request);

        if (!outcome.IsSuccess()) {
            switch (outcome.GetError().GetErrorType()) {
                case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                    return Error::result(ErrorType::ACCESS_DENIED);
                case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                    return Error::result(ErrorType::INVALID_BUCKET_NAME);
                default:
                    return Error::result(ErrorType::CANNOT_LIST_FILES);
            }
        } else {
            Aws::Vector<Aws::S3Crt::Model::Object> objects =
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
    size_t lastSlash = commonPrefix.find_last_of('/');
    size_t secondLastSlash = commonPrefix.find_last_of('/', lastSlash - 1);

    return commonPrefix.substr(secondLastSlash + 1, lastSlash - secondLastSlash - 1);
}

template <typename T>
Result<void> TuringS3Client<T>::listFolders(const std::string& bucketName,
                                            std::vector<std::string>& folderResults,
                                            const std::string& prefix) {
    if (prefix.back() != '/') {
        return Error::result(ErrorType::INVALID_PREFIX_STRING);
    }

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucketName).WithPrefix(prefix).WithDelimiter("/");

    Aws::String continuationToken; // Used for pagination.
    Aws::Vector<Aws::S3Crt::Model::CommonPrefix> allFolders;

    do {
        if (!continuationToken.empty()) {
            request.SetContinuationToken(continuationToken);
        }

        auto outcome = _client.ListObjectsV2(request);

        if (!outcome.IsSuccess()) {
            switch (outcome.GetError().GetErrorType()) {
                case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                    return Error::result(ErrorType::ACCESS_DENIED);
                case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                    return Error::result(ErrorType::INVALID_BUCKET_NAME);
                default:
                    return Error::result(ErrorType::CANNOT_LIST_FOLDERS);
            }
        } else {
            Aws::Vector<Aws::S3Crt::Model::CommonPrefix> folders =
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
Result<void> TuringS3Client<T>::uploadFile(const Aws::String& filePath,
                                           const Aws::String& bucketName,
                                           const Aws::String& keyName) {
    std::error_code ec;
    if (!std::filesystem::exists(filePath, ec)) {
        if (ec) {
            return Error::result(ErrorType::FILE_SYSTEM_ERROR);
        }
        return Error::result(ErrorType::FILE_NOT_FOUND);
    }

    Aws::S3Crt::Model::PutObjectRequest request;
    request.WithBucket(bucketName).WithKey(keyName).SetIfNoneMatch("*");
    std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::FStream>("TuringS3Client", filePath.c_str(), std::ios_base::in | std::ios_base::binary);

    if (!*inputData) {
        return Error::result(ErrorType::CANNOT_OPEN_FILE);
    }

    request.SetBody(inputData);

    Aws::S3Crt::Model::PutObjectOutcome outcome = _client.PutObject(request);

    if (!outcome.IsSuccess()) {
        switch (outcome.GetError().GetErrorType()) {
            case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                return Error::result(ErrorType::ACCESS_DENIED);
            case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                return Error::result(ErrorType::INVALID_BUCKET_NAME);
            default:
                if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
                    return Error::result(ErrorType::FILE_EXISTS);
                }
                return Error::result(ErrorType::CANNOT_UPLOAD_FILE);
        }
    }

    return {};
}

template <typename T>
Result<void> TuringS3Client<T>::downloadFile(const Aws::String& fileName,
                                             const Aws::String& bucketName,
                                             const Aws::String& keyName) {
    Aws::S3Crt::Model::GetObjectRequest request;
    request.WithBucket(bucketName).WithKey(keyName);

    Aws::S3Crt::Model::GetObjectOutcome outcome = _client.GetObject(request);

    if (!outcome.IsSuccess()) {
        switch (outcome.GetError().GetErrorType()) {
            case Aws::S3Crt::S3CrtErrors::ACCESS_DENIED:
                return Error::result(ErrorType::ACCESS_DENIED);
            case Aws::S3Crt::S3CrtErrors::NO_SUCH_KEY:
                return Error::result(ErrorType::INVALID_KEY_NAME);
            case Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET:
                return Error::result(ErrorType::INVALID_BUCKET_NAME);
            default:
                return Error::result(ErrorType::CANNOT_DOWNLOAD_FILE);
        }
    }

    std::ofstream outputFileStream(fileName.c_str(), std::ios_base::out | std::ios_base::binary);

    if (!outputFileStream.is_open()) {
        std::error_code ec;
        std::filesystem::remove(fileName, ec);
        if (ec) {
            return Error::result(ErrorType::FILE_SYSTEM_ERROR);
        }
        return Error::result(ErrorType::CANNOT_OPEN_FILE);
    }
    outputFileStream << outcome.GetResult().GetBody().rdbuf();
    outputFileStream.flush();
    outputFileStream.close();

    return {};
}

// Need trailing forward slash at the end of the prefix parameter!
template <typename T>
Result<void> TuringS3Client<T>::uploadDirectory(const std::string& directory,
                                                const std::string& bucketName,
                                                const std::string& prefix) {
    if (prefix.back() != '/') {
        return Error::result(ErrorType::INVALID_PREFIX_STRING);
    }

    std::error_code ec;
    if (!std::filesystem::exists(directory, ec)) {
        if (ec) {
            return Error::result(ErrorType::FILE_SYSTEM_ERROR);
        }
        return Error::result(ErrorType::DIRECTORY_NOT_FOUND);
    }

    Result<void> directoryRes;

    auto visitor = [&prefix, &bucketName, this, &directoryRes](const Aws::FileSystem::DirectoryTree*, const Aws::FileSystem::DirectoryEntry& entry) {
        if (entry && entry.fileType == Aws::FileSystem::FileType::File) {
            // Change to fmt
            Aws::StringStream ssKey;
            Aws::String relativePath = entry.relativePath;

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
            case ErrorType::CANNOT_UPLOAD_FILE:
            case ErrorType::CANNOT_OPEN_FILE:
            case ErrorType::INVALID_KEY_NAME:
                return Error::result(ErrorType::CANNOT_UPLOAD_DIRECTORY);
            case ErrorType::FILE_EXISTS:
                return Error::result(ErrorType::DIRECTORY_EXISTS);
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
Result<void> TuringS3Client<T>::downloadDirectory(const std::string& directory,
                                                  const std::string& bucketName,
                                                  const std::string& prefix) {
    if (prefix.back() != '/') {
        return Error::result(ErrorType::INVALID_PREFIX_STRING);
    }

    std::error_code ec;
    std::filesystem::create_directory(directory, ec);
    if (ec) {
        return Error::result(ErrorType::FILE_SYSTEM_ERROR);
    }

    Aws::String downloadPath;
    std::vector<std::string> results;

    if (auto res = listKeys(bucketName, results, prefix); !res) {
        if (res.error().getType() == ErrorType::CANNOT_LIST_KEYS) {
            return Error::result(ErrorType::CANNOT_DOWNLOAD_DIRECTORY);
        } else {
            return res;
        }
    }

    if (results.empty()) {
        return Error::result(ErrorType::INVALID_DIRECTORY_NAME);
    }

    for (const auto& result : results) {
        if (!isFolder(result)) {
            auto pos = result.find(prefix) + prefix.size();
            downloadPath = fmt::format("{}/{}", directory, result.substr(pos));

            auto lastDelimiter = downloadPath.find_last_of('/');
            std::filesystem::create_directories(downloadPath.substr(0, lastDelimiter).c_str(), ec); // Add error handling
            if (ec) {
                return Error::result(ErrorType::FILE_SYSTEM_ERROR);
            }
            if (auto res = downloadFile(downloadPath, bucketName, result); !res) {
                switch (res.error().getType()) {
                    case ErrorType::CANNOT_DOWNLOAD_FILE:
                    case ErrorType::CANNOT_OPEN_FILE:
                    case ErrorType::INVALID_KEY_NAME:
                        return Error::result(ErrorType::CANNOT_DOWNLOAD_DIRECTORY);
                    default:
                        return res;
                }
            }
        }
    }

    return {};
}

template class TuringS3Client<AwsS3ClientWrapper<>>;
template class TuringS3Client<AwsS3ClientWrapper<MockS3Client>>;

}
