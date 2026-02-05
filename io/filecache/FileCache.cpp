#include "FileCache.h"

#include <spdlog/spdlog.h>

#include "MinioS3ClientWrapper.h"
#include "MockS3Client.h"

using namespace db;

template <typename ClientType>
FileCache<ClientType>::FileCache(const fs::Path& graphDir,
                                 const fs::Path& dataDir,
                                 S3::TuringS3Client<ClientType>& clientWrapper)
    :_graphsDir(graphDir),
    _dataDir(dataDir),
    _s3Client(clientWrapper)
{
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::listLocalGraphs(std::vector<fs::Path>& graphs) {
    const auto list = fs::Path(_graphsDir).listDir();
    if (!list) {
        return FileCacheError::result(FileCacheErrorType::LIST_LOCAL_GRAPHS_FAILED);
    }

    for (const auto& path : list.value()) {
        graphs.push_back(path);
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::listGraphs(std::vector<std::string>& graphs) {
    const auto prefix = fmt::format("{}/graphs/", _userId);
    if (auto res = _s3Client.listFolders(_bucketName, prefix, graphs); !res) {
        return FileCacheError::result(FileCacheErrorType::LIST_GRAPHS_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::loadGraph(std::string_view graphName) {
    const bool exists = (_graphsDir / graphName).exists();
    if (exists) {
        return {};
    }

    const std::string graphDirectory = fmt::format("{}/{}/", _graphsDir.c_str(), graphName);
    const std::string s3Prefix = fmt::format("{}/graphs/{}/", _userId, graphName);
    if (auto res = _s3Client.downloadDirectory(graphDirectory, _bucketName, s3Prefix); !res) {
        return FileCacheError::result(FileCacheErrorType::GRAPH_LOAD_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::saveGraph(std::string_view graphName) {
    const bool exists = (_graphsDir / graphName).exists();
    if (!exists) {
        return FileCacheError::result(FileCacheErrorType::FAILED_TO_FIND_GRAPH);
    }

    const std::string graphDirectory = fmt::format("{}/{}/", _graphsDir.c_str(), graphName);
    const std::string s3Prefix = fmt::format("{}/graphs/{}/", _userId, graphName);
    if (auto res = _s3Client.uploadDirectory(graphDirectory, _bucketName, s3Prefix); !res) {
        return FileCacheError::result(FileCacheErrorType::GRAPH_SAVE_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::listData(std::vector<std::string>& files,
                                                      std::vector<std::string>& folders,
                                                      std::string_view dir) {
    const auto prefix = fmt::format("{}/data/{}/", _userId, dir);
    if (auto res = _s3Client.listFolders(_bucketName, prefix, folders); !res) {
        return FileCacheError::result(FileCacheErrorType::LIST_DATA_FAILED, res.error());
    }
    if (auto res = _s3Client.listFiles(_bucketName, prefix, files); !res) {
        return FileCacheError::result(FileCacheErrorType::LIST_DATA_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::listData(std::vector<std::string>& files,
                                                      std::vector<std::string>& folders) {
    const auto prefix = fmt::format("{}/data/", _userId);
    std::string emptyString;
    return listData(files, folders, emptyString);
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::listLocalData(std::vector<fs::Path>& files, std::vector<fs::Path>& folders, std::string_view dir) {
    const auto fullPath = _dataDir / dir;
    if (!fullPath.exists()) {
        return FileCacheError::result(FileCacheErrorType::DIRECTORY_DOES_NOT_EXIST);
    }
    const auto list = fullPath.listDir();
    if (!list) {
        return FileCacheError::result(FileCacheErrorType::LIST_LOCAL_DATA_FAILED);
    }

    for (const auto& path : list.value()) {
        const auto res = (path).getFileInfo();

        if (res.value()._type == fs::FileType::File) {
            files.push_back(path);
        }
        if (res.value()._type == fs::FileType::Directory) {
            folders.push_back(path);
        }
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::listLocalData(std::vector<fs::Path>& files, std::vector<fs::Path>& folders) {
    std::string emptyString;
    return listLocalData(files, folders, emptyString);
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::saveDataFile(std::string_view filePath) {
    const auto fileInfo = (_dataDir / filePath).getFileInfo();
    if (!fileInfo.has_value()) {
        return FileCacheError::result(FileCacheErrorType::FAILED_TO_FIND_DATA_FILE);
    }
    if (fileInfo->_type != fs::FileType::File) {
        return FileCacheError::result(FileCacheErrorType::FILE_PATH_IS_DIRECTORY);
    }

    const std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), filePath);
    const std::string s3Prefix = fmt::format("{}/data/{}/", _userId, filePath);
    if (auto res = _s3Client.uploadFile(fullLocalPath, _bucketName, s3Prefix); !res) {
        return FileCacheError::result(FileCacheErrorType::DATA_FILE_SAVE_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::loadDataFile(std::string_view filePath) {
    const auto info = (_dataDir / filePath).getFileInfo();
    if (info.has_value()) {
        if (info->_type != fs::FileType::File) {
            return FileCacheError::result(FileCacheErrorType::FILE_PATH_IS_DIRECTORY);
        }
        return {};
    }

    const std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), filePath);
    const std::string s3prefix = fmt::format("{}/data/{}/", _userId, filePath);
    if (auto res = _s3Client.downloadFile(fullLocalPath, _bucketName, s3prefix); !res) {
        return FileCacheError::result(FileCacheErrorType::DATA_FILE_LOAD_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::saveDataDirectory(std::string_view directoryPath) {
    const auto info = (_dataDir / directoryPath).getFileInfo();
    if (!info.has_value()) {
        return FileCacheError::result(FileCacheErrorType::FAILED_TO_FIND_DATA_DIRECTORY);
    }

    if (info->_type != fs::FileType::Directory) {
        return FileCacheError::result(FileCacheErrorType::DIRECTORY_PATH_IS_FILE);
    }

    const std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), directoryPath);
    const std::string dataDirPrefix = fmt::format("{}/data/{}/", _userId, directoryPath);

    if (auto res = _s3Client.uploadDirectory(fullLocalPath, _bucketName, dataDirPrefix); !res) {
        return FileCacheError::result(FileCacheErrorType::DATA_DIRECTORY_SAVE_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::loadDataDirectory(std::string_view directoryPath) {
    const auto info = (_dataDir / directoryPath).getFileInfo();

    if (info.has_value()) {
        if (info->_type != fs::FileType::Directory) {
            return FileCacheError::result(FileCacheErrorType::DIRECTORY_PATH_IS_FILE);
        }
        return {};
    }

    const std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), directoryPath);
    const std::string prefix = fmt::format("{}/data/{}/", _userId, directoryPath);
    if (auto res = _s3Client.downloadDirectory(fullLocalPath, _bucketName, prefix); !res) {
        return FileCacheError::result(FileCacheErrorType::DATA_DIRECTORY_LOAD_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
FileCacheResult<void> FileCache<ClientType>::initS3Storage() {
    // Check if bucket exists
    auto existsResult = _s3Client.bucketExists(_bucketName);
    if (!existsResult) {
        return FileCacheError::result(FileCacheErrorType::S3_STORAGE_INIT_FAILED,
                                      existsResult.error());
    }

    if (!existsResult.value()) {
        spdlog::error("Bucket '{}' does not exist", _bucketName);
        return FileCacheError::result(FileCacheErrorType::S3_STORAGE_INIT_FAILED);
    }

    // Create directory structure: {userId}/
    const std::string userPrefix = fmt::format("{}/", _userId);
    spdlog::info("Creating directory marker: {}", userPrefix);
    if (auto res = _s3Client.createDirectoryMarker(_bucketName, userPrefix); !res) {
        return FileCacheError::result(FileCacheErrorType::S3_STORAGE_INIT_FAILED,
                                      res.error());
    }

    // Create {userId}/graphs/
    const std::string graphsPrefix = fmt::format("{}/graphs/", _userId);
    spdlog::info("Creating directory marker: {}", graphsPrefix);
    if (auto res = _s3Client.createDirectoryMarker(_bucketName, graphsPrefix); !res) {
        return FileCacheError::result(FileCacheErrorType::S3_STORAGE_INIT_FAILED,
                                      res.error());
    }

    // Create {userId}/data/
    const std::string dataPrefix = fmt::format("{}/data/", _userId);
    spdlog::info("Creating directory marker: {}", dataPrefix);
    if (auto res = _s3Client.createDirectoryMarker(_bucketName, dataPrefix); !res) {
        return FileCacheError::result(FileCacheErrorType::S3_STORAGE_INIT_FAILED,
                                      res.error());
    }

    spdlog::info("S3 storage initialized successfully");
    return {};
}

template <typename ClientType>
void FileCache<ClientType>::setBucketName(const std::string& bucketName) {
    _bucketName = bucketName;
}

template <typename ClientType>
const std::string& FileCache<ClientType>::getBucketName() const {
    return _bucketName;
}

template <typename ClientType>
S3::TuringS3Client<ClientType>& FileCache<ClientType>::getS3Client() {
    return _s3Client;
}

template class db::FileCache<S3::MinioS3ClientWrapper>;
template class db::FileCache<S3::MockS3Client>;
