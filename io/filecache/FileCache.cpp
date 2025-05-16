#include "FileCache.h"
#include "AwsS3ClientWrapper.h"
#include "MockS3Client.h"

#include <spdlog/spdlog.h>

namespace db {

template <typename ClientType>
FileCache<ClientType>::FileCache(fs::Path& graphDir, fs::Path& dataDir, ClientType& clientWrapper)
    :_graphsDir(graphDir),
    _dataDir(dataDir),
    _s3Client(clientWrapper)
{
}

template <typename ClientType>
Result<void> FileCache<ClientType>::listLocalGraphs(std::vector<fs::Path>& graphs) {
    const auto list = fs::Path(_graphsDir).listDir();
    if (!list) {

        return Error::result(ErrorType::LIST_LOCAL_GRAPHS_FAILED);
    }

    for (const auto& path : list.value()) {
        graphs.push_back(path);
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::listGraphs(std::vector<std::string>& graphs) {
    auto prefix = fmt::format("{}/graphs/", _userId);
    if (auto res = _s3Client.listFolders(_bucketName, graphs, prefix); !res) {
        return Error::result(ErrorType::LIST_GRAPHS_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::loadGraph(std::string_view graphName) {
    const bool exists = (_graphsDir / graphName).exists();
    if (exists) {
        return {};
    }

    std::string graphDirectory = fmt::format("{}/{}/", _graphsDir.c_str(), graphName);
    std::string s3Prefix = fmt::format("{}/graphs/{}/", _userId, graphName);
    if (auto res = _s3Client.downloadDirectory(graphDirectory, _bucketName, s3Prefix); !res) {
        return Error::result(ErrorType::GRAPH_LOAD_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::saveGraph(std::string_view graphName) {
    const bool exists = (_graphsDir / graphName).exists();
    if (!exists) {
        return Error::result(ErrorType::FAILED_TO_FIND_GRAPH);
    }

    std::string graphDirectory = fmt::format("{}/{}/", _graphsDir.c_str(), graphName);
    std::string s3Prefix = fmt::format("{}/graphs/{}/", _userId, graphName);
    if (auto res = _s3Client.uploadDirectory(graphDirectory, _bucketName, s3Prefix); !res) {
        return Error::result(ErrorType::GRAPH_SAVE_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::listData(std::vector<std::string>& files,
                                             std::vector<std::string>& folders,
                                             std::string_view dir) {
    auto prefix = fmt::format("{}/data/{}/", _userId, dir);
    if (auto res = _s3Client.listFolders(_bucketName, folders, prefix); !res) {
        return Error::result(ErrorType::LIST_DATA_FAILED, res.error());
    }
    if (auto res = _s3Client.listFiles(_bucketName, files, prefix); !res) {
        return Error::result(ErrorType::LIST_DATA_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::listData(std::vector<std::string>& files,
                                             std::vector<std::string>& folders) {
    auto prefix = fmt::format("{}/data/", _userId);
    std::string emptyString;
    return listData(files, folders, emptyString);
}

template <typename ClientType>
Result<void> FileCache<ClientType>::listLocalData(std::vector<fs::Path>& files, std::vector<fs::Path>& folders, std::string_view dir) {
    const auto fullPath = _dataDir / dir;
    if (!fullPath.exists()) {
        return Error::result(ErrorType::DIRECTORY_DOES_NOT_EXIST);
    }
    const auto list = fullPath.listDir();
    if (!list) {
        return Error::result(ErrorType::LIST_LOCAL_DATA_FAILED);
    }

    for (const auto& path : list.value()) {
        auto res = (path).getFileInfo();

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
Result<void> FileCache<ClientType>::listLocalData(std::vector<fs::Path>& files, std::vector<fs::Path>& folders) {
    std::string emptyString;
    return listLocalData(files, folders, emptyString);
}

template <typename ClientType>
Result<void> FileCache<ClientType>::saveDataFile(std::string_view filePath) {
    const auto fileInfo = (_dataDir / filePath).getFileInfo();
    if (!fileInfo.has_value()) {
        return Error::result(ErrorType::FAILED_TO_FIND_DATA_FILE);
    }
    if (fileInfo->_type != fs::FileType::File) {
        return Error::result(ErrorType::FILE_PATH_IS_DIRECTORY);
    }

    std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), filePath);
    std::string s3Prefix = fmt::format("{}/data/{}/", _userId, filePath);
    if (auto res = _s3Client.uploadFile(fullLocalPath, _bucketName, s3Prefix); !res) {
        return Error::result(ErrorType::DATA_FILE_SAVE_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::loadDataFile(std::string_view filePath) {
    const auto info = (_dataDir / filePath).getFileInfo();
    if (info.has_value()) {
        if (info->_type != fs::FileType::File) {
            return Error::result(ErrorType::FILE_PATH_IS_DIRECTORY);
        }
        return {};
    }

    std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), filePath);
    std::string s3prefix = fmt::format("{}/data/{}/", _userId, filePath);
    if (auto res = _s3Client.downloadFile(fullLocalPath, _bucketName, s3prefix); !res) {
        return Error::result(ErrorType::DATA_FILE_LOAD_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::saveDataDirectory(std::string_view directoryPath) {
    const auto info = (_dataDir / directoryPath).getFileInfo();
    if (!info.has_value()) {
        return Error::result(ErrorType::FAILED_TO_FIND_DATA_DIRECTORY);
    }

    if (info->_type != fs::FileType::Directory) {
        return Error::result(ErrorType::DIRECTORY_PATH_IS_FILE);
    }

    std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), directoryPath);
    std::string dataDirPrefix = fmt::format("{}/data/{}/", _userId, directoryPath);

    if (auto res = _s3Client.uploadDirectory(fullLocalPath, _bucketName, dataDirPrefix); !res) {
        return Error::result(ErrorType::DATA_DIRECTORY_SAVE_FAILED, res.error());
    }

    return {};
}

template <typename ClientType>
Result<void> FileCache<ClientType>::loadDataDirectory(std::string_view directoryPath) {
    const auto info = (_dataDir / directoryPath).getFileInfo();

    if (info.has_value()) {
        if (info->_type != fs::FileType::Directory) {
            return Error::result(ErrorType::DIRECTORY_PATH_IS_FILE);
        }
        return {};
    }

    std::string fullLocalPath = fmt::format("{}/{}", _dataDir.c_str(), directoryPath);
    std::string prefix = fmt::format("{}/data/{}/", _userId, directoryPath);
    if (auto res = _s3Client.downloadDirectory(fullLocalPath, _bucketName, prefix); !res) {
        return Error::result(ErrorType::DATA_DIRECTORY_LOAD_FAILED, res.error());
    }

    return {};
}

template class FileCache<S3::AwsS3ClientWrapper<>>;
template class FileCache<S3::AwsS3ClientWrapper<S3::MockS3Client>>;
}
