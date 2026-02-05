#include "TuringS3Client.h"

#include <filesystem>
#include <fstream>

#include <miniocpp/client.h>

#include "MinioS3ClientWrapper.h"
#include "MockS3Client.h"
#include "spdlog/spdlog.h"

using namespace S3;

namespace {

S3ClientErrorType mapMinioError(const std::string& code, int statusCode) {
    if (code == "AccessDenied" || code == "SignatureDoesNotMatch" ||
        code == "InvalidAccessKeyId") {
        return S3ClientErrorType::ACCESS_DENIED;
    }
    if (code == "NoSuchBucket") {
        return S3ClientErrorType::INVALID_BUCKET_NAME;
    }
    if (code == "NoSuchKey") {
        return S3ClientErrorType::INVALID_KEY_NAME;
    }
    if (statusCode == 412) {
        return S3ClientErrorType::FILE_EXISTS;
    }
    return S3ClientErrorType::UNKNOWN;
}

std::string extractFileNameFromKey(const std::string& key) {
    const size_t lastSlash = key.find_last_of('/');
    return key.substr(lastSlash + 1);
}

std::string commonPrefixToFolderName(const std::string& commonPrefix) {
    const size_t lastSlash = commonPrefix.find_last_of('/');
    const size_t secondLastSlash = commonPrefix.find_last_of('/', lastSlash - 1);
    return commonPrefix.substr(secondLastSlash + 1, lastSlash - secondLastSlash - 1);
}

bool isFolder(const std::string& path) {
    return (path.find_last_of('/') == path.size() - 1);
}

}

template <typename ClientType>
TuringS3Client<ClientType>::TuringS3Client(ClientType&& client)
    : _client(std::move(client))
{
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::listKeys(
    const std::string& bucketName,
    const std::string& prefix,
    std::vector<std::string>& keyResults) {

    if (!prefix.empty() && prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    minio::s3::ListObjectsArgs args;
    args.bucket = bucketName;
    args.prefix = prefix;
    args.recursive = true;

    minio::s3::ListObjectsResult result = _client.client().ListObjects(args);
    for (; result; result++) {
        minio::s3::Item item = *result;
        if (!item) {
            const auto errorType = mapMinioError(item.code, item.status_code);
            if (errorType == S3ClientErrorType::ACCESS_DENIED) {
                return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
            }
            if (errorType == S3ClientErrorType::INVALID_BUCKET_NAME) {
                return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
            }
            return S3ClientError::result(S3ClientErrorType::CANNOT_LIST_KEYS);
        }
        if (!item.is_prefix) {
            keyResults.push_back(item.name);
        }
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::listFiles(
    const std::string& bucketName,
    const std::string& prefix,
    std::vector<std::string>& keyResults) {

    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    minio::s3::ListObjectsArgs args;
    args.bucket = bucketName;
    args.prefix = prefix;
    args.delimiter = "/";
    args.recursive = false;

    minio::s3::ListObjectsResult result = _client.client().ListObjects(args);
    for (; result; result++) {
        minio::s3::Item item = *result;
        if (!item) {
            const auto err = item.Error();
            const auto errorType = mapMinioError(item.code, item.status_code);
            if (errorType == S3ClientErrorType::ACCESS_DENIED) {
                return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
            }
            if (errorType == S3ClientErrorType::INVALID_BUCKET_NAME) {
                return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
            }
            return S3ClientError::result(S3ClientErrorType::CANNOT_LIST_FILES);
        }
        if (!item.is_prefix) {
            keyResults.push_back(extractFileNameFromKey(item.name));
        }
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::listFolders(
    const std::string& bucketName,
    const std::string& prefix,
    std::vector<std::string>& folderResults) {

    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    minio::s3::ListObjectsArgs args;
    args.bucket = bucketName;
    args.prefix = prefix;
    args.delimiter = "/";
    args.recursive = false;

    minio::s3::ListObjectsResult result = _client.client().ListObjects(args);
    for (; result; result++) {
        minio::s3::Item item = *result;
        if (!item) {
            const auto err = item.Error();
            const auto errorType = mapMinioError(item.code, item.status_code);
            if (errorType == S3ClientErrorType::ACCESS_DENIED) {
                return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
            }
            if (errorType == S3ClientErrorType::INVALID_BUCKET_NAME) {
                return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
            }
            return S3ClientError::result(S3ClientErrorType::CANNOT_LIST_FOLDERS);
        }
        if (item.is_prefix) {
            folderResults.push_back(commonPrefixToFolderName(item.name));
        }
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::uploadFile(
    const std::string& filePath,
    const std::string& bucketName,
    const std::string& keyName) {

    std::error_code ec;
    if (!std::filesystem::exists(filePath, ec)) {
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }
        return S3ClientError::result(S3ClientErrorType::FILE_NOT_FOUND);
    }

    minio::s3::UploadObjectArgs args;
    args.bucket = bucketName;
    args.object = keyName;
    args.filename = filePath;

    minio::s3::UploadObjectResponse resp = _client.client().UploadObject(args);

    if (!resp) {
        const auto errorType = mapMinioError(resp.code, resp.status_code);
        if (errorType == S3ClientErrorType::ACCESS_DENIED) {
            return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
        }
        if (errorType == S3ClientErrorType::INVALID_BUCKET_NAME) {
            return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
        }
        if (errorType == S3ClientErrorType::FILE_EXISTS) {
            return S3ClientError::result(S3ClientErrorType::FILE_EXISTS);
        }
        return S3ClientError::result(S3ClientErrorType::CANNOT_UPLOAD_FILE);
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::downloadFile(
    const std::string& fileName,
    const std::string& bucketName,
    const std::string& keyName) {

    minio::s3::DownloadObjectArgs args;
    args.bucket = bucketName;
    args.object = keyName;
    args.filename = fileName;
    args.overwrite = true;

    minio::s3::DownloadObjectResponse resp = _client.client().DownloadObject(args);

    if (!resp) {
        const auto errorType = mapMinioError(resp.code, resp.status_code);
        if (errorType == S3ClientErrorType::ACCESS_DENIED) {
            return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
        }
        if (errorType == S3ClientErrorType::INVALID_KEY_NAME) {
            return S3ClientError::result(S3ClientErrorType::INVALID_KEY_NAME);
        }
        if (errorType == S3ClientErrorType::INVALID_BUCKET_NAME) {
            return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
        }
        return S3ClientError::result(S3ClientErrorType::CANNOT_DOWNLOAD_FILE);
    }

    std::ifstream testFile(fileName);
    if (!testFile.is_open()) {
        std::error_code ec;
        std::filesystem::remove(fileName, ec);
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }
        return S3ClientError::result(S3ClientErrorType::CANNOT_OPEN_FILE);
    }
    testFile.close();

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::uploadDirectory(
    const std::string& directory,
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

    const std::filesystem::path dirPath(directory);

    for (const auto& entry : std::filesystem::recursive_directory_iterator(dirPath, ec)) {
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }

        if (entry.is_regular_file()) {
            const std::filesystem::path relativePath =
                std::filesystem::relative(entry.path(), dirPath, ec);
            if (ec) {
                return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
            }

            const std::string keyName = prefix + relativePath.string();
            if (auto res = uploadFile(entry.path().string(), bucketName, keyName); !res) {
                switch (res.error().getType()) {
                    case S3ClientErrorType::CANNOT_UPLOAD_FILE:
                    case S3ClientErrorType::CANNOT_OPEN_FILE:
                    case S3ClientErrorType::INVALID_KEY_NAME:
                        return S3ClientError::result(S3ClientErrorType::CANNOT_UPLOAD_DIRECTORY);
                    case S3ClientErrorType::FILE_EXISTS:
                        return S3ClientError::result(S3ClientErrorType::DIRECTORY_EXISTS);
                    default:
                        return res;
                }
            }
        }
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::downloadDirectory(
    const std::string& directory,
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

    std::string downloadPath;
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
            std::filesystem::create_directories(downloadPath.substr(0, lastDelimiter).c_str(), ec);
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

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::listKeys(
    const std::string& bucketName,
    const std::string& prefix,
    std::vector<std::string>& keyResults) {

    if (!prefix.empty() && prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    const auto& listResult = _client.getListResult();
    if (!listResult.success) {
        return S3ClientError::result(listResult.errorType);
    }

    for (const auto& key : listResult.keys) {
        keyResults.push_back(key);
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::listFiles(
    const std::string& bucketName,
    const std::string& prefix,
    std::vector<std::string>& keyResults) {

    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    const auto& listResult = _client.getListResult();
    if (!listResult.success) {
        return S3ClientError::result(listResult.errorType);
    }

    for (const auto& key : listResult.keys) {
        keyResults.push_back(extractFileNameFromKey(key));
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::listFolders(
    const std::string& bucketName,
    const std::string& prefix,
    std::vector<std::string>& folderResults) {

    if (prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    const auto& listResult = _client.getListResult();
    if (!listResult.success) {
        return S3ClientError::result(listResult.errorType);
    }

    for (const auto& commonPrefix : listResult.commonPrefixes) {
        folderResults.push_back(commonPrefixToFolderName(commonPrefix));
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::uploadFile(
    const std::string& filePath,
    const std::string& bucketName,
    const std::string& keyName) {

    std::error_code ec;
    if (!std::filesystem::exists(filePath, ec)) {
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }
        return S3ClientError::result(S3ClientErrorType::FILE_NOT_FOUND);
    }

    std::ifstream inputFile(filePath, std::ios_base::in | std::ios_base::binary);
    if (!inputFile) {
        return S3ClientError::result(S3ClientErrorType::CANNOT_OPEN_FILE);
    }

    const auto& uploadResult = _client.getUploadResult();
    if (!uploadResult.success) {
        if (uploadResult.statusCode == 412) {
            return S3ClientError::result(S3ClientErrorType::FILE_EXISTS);
        }
        return S3ClientError::result(uploadResult.errorType);
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::downloadFile(
    const std::string& fileName,
    const std::string& bucketName,
    const std::string& keyName) {

    const auto& downloadResult = _client.getDownloadResult();
    if (!downloadResult.success) {
        return S3ClientError::result(downloadResult.errorType);
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
    outputFileStream << downloadResult.content;
    outputFileStream.flush();
    outputFileStream.close();

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::uploadDirectory(
    const std::string& directory,
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

    const std::filesystem::path dirPath(directory);

    for (const auto& entry : std::filesystem::recursive_directory_iterator(dirPath, ec)) {
        if (ec) {
            return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
        }

        if (entry.is_regular_file()) {
            const std::filesystem::path relativePath =
                std::filesystem::relative(entry.path(), dirPath, ec);
            if (ec) {
                return S3ClientError::result(S3ClientErrorType::FILE_SYSTEM_ERROR);
            }

            const std::string keyName = prefix + relativePath.string();
            if (auto res = uploadFile(entry.path().string(), bucketName, keyName); !res) {
                switch (res.error().getType()) {
                    case S3ClientErrorType::CANNOT_UPLOAD_FILE:
                    case S3ClientErrorType::CANNOT_OPEN_FILE:
                    case S3ClientErrorType::INVALID_KEY_NAME:
                        return S3ClientError::result(S3ClientErrorType::CANNOT_UPLOAD_DIRECTORY);
                    case S3ClientErrorType::FILE_EXISTS:
                        return S3ClientError::result(S3ClientErrorType::DIRECTORY_EXISTS);
                    default:
                        return res;
                }
            }
        }
    }

    return {};
}

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::downloadDirectory(
    const std::string& directory,
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

    std::string downloadPath;
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
            std::filesystem::create_directories(downloadPath.substr(0, lastDelimiter).c_str(), ec);
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

template <>
S3ClientResult<bool> TuringS3Client<MinioS3ClientWrapper>::bucketExists(
    const std::string& bucketName) {

    minio::s3::BucketExistsArgs args;
    args.bucket = bucketName;

    minio::s3::BucketExistsResponse resp = _client.client().BucketExists(args);

    if (!resp) {
        const auto errorType = mapMinioError(resp.code, resp.status_code);
        if (errorType == S3ClientErrorType::ACCESS_DENIED) {
            return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
        }
        return S3ClientError::result(S3ClientErrorType::CANNOT_CHECK_BUCKET);
    }

    return resp.exist;
}

template <>
S3ClientResult<void> TuringS3Client<MinioS3ClientWrapper>::createDirectoryMarker(
    const std::string& bucketName,
    const std::string& prefix) {

    if (prefix.empty() || prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    std::istringstream emptyStream("");
    minio::s3::PutObjectArgs args(emptyStream, 0, 0);

    args.bucket = bucketName;
    args.object = prefix;

    minio::s3::PutObjectResponse resp = _client.client().PutObject(args);

    if (!resp) {
        const auto errorType = mapMinioError(resp.code, resp.status_code);
        if (errorType == S3ClientErrorType::ACCESS_DENIED) {
            return S3ClientError::result(S3ClientErrorType::ACCESS_DENIED);
        }
        if (errorType == S3ClientErrorType::INVALID_BUCKET_NAME) {
            return S3ClientError::result(S3ClientErrorType::INVALID_BUCKET_NAME);
        }
        return S3ClientError::result(S3ClientErrorType::CANNOT_CREATE_DIRECTORY_MARKER);
    }

    return {};
}

template <>
S3ClientResult<bool> TuringS3Client<MockS3Client>::bucketExists(
    const std::string& bucketName) {

    const auto& listResult = _client.getListResult();
    if (!listResult.success) {
        return S3ClientError::result(listResult.errorType);
    }
    return true;
}

template <>
S3ClientResult<void> TuringS3Client<MockS3Client>::createDirectoryMarker(
    const std::string& bucketName,
    const std::string& prefix) {

    if (prefix.empty() || prefix.back() != '/') {
        return S3ClientError::result(S3ClientErrorType::INVALID_PREFIX_STRING);
    }

    const auto& uploadResult = _client.getUploadResult();
    if (!uploadResult.success) {
        return S3ClientError::result(uploadResult.errorType);
    }
    return {};
}

template class S3::TuringS3Client<MinioS3ClientWrapper>;
template class S3::TuringS3Client<MockS3Client>;
