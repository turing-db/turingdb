#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "BasicResult.h"
#include "EnumToString.h"
#include "S3ClientResult.h"

namespace db {

class Path;

enum class FileCacheErrorType : uint8_t {
    UNKNOWN = 0,

    LIST_LOCAL_GRAPHS_FAILED,
    LIST_GRAPHS_FAILED,
    GRAPH_LOAD_FAILED,
    GRAPH_SAVE_FAILED,
    FAILED_TO_FIND_GRAPH,
    LIST_LOCAL_DATA_FAILED,
    LIST_DATA_FAILED,
    DATA_FILE_LOAD_FAILED,
    DATA_FILE_SAVE_FAILED,
    FAILED_TO_FIND_DATA_FILE,
    FILE_PATH_IS_DIRECTORY,
    DIRECTORY_DOES_NOT_EXIST,
    DATA_DIRECTORY_LOAD_FAILED,
    DATA_DIRECTORY_SAVE_FAILED,
    FAILED_TO_FIND_DATA_DIRECTORY,
    DIRECTORY_PATH_IS_FILE,
    _SIZE,
};

using ErrorTypeDescription = EnumToString<FileCacheErrorType>::Create<
    EnumStringPair<FileCacheErrorType::UNKNOWN, "Unknown">,
    EnumStringPair<FileCacheErrorType::LIST_LOCAL_GRAPHS_FAILED, "Failed to list graphs available in the local file cache">,
    EnumStringPair<FileCacheErrorType::LIST_GRAPHS_FAILED, "Failed to list graphs available in remote storage">,
    EnumStringPair<FileCacheErrorType::GRAPH_LOAD_FAILED, "Failed to load graph">,
    EnumStringPair<FileCacheErrorType::GRAPH_SAVE_FAILED, "Failed to save graph">,
    EnumStringPair<FileCacheErrorType::FAILED_TO_FIND_GRAPH, "Failed to find graph in local file cache">,
    EnumStringPair<FileCacheErrorType::LIST_LOCAL_DATA_FAILED, "Failed to list data available in the local file cache">,
    EnumStringPair<FileCacheErrorType::LIST_DATA_FAILED, "Failed to list data available in remote storage">,
    EnumStringPair<FileCacheErrorType::DATA_FILE_LOAD_FAILED, "Failed to load file from remote storage">,
    EnumStringPair<FileCacheErrorType::DATA_FILE_SAVE_FAILED, "Failed to save file to remote storage">,
    EnumStringPair<FileCacheErrorType::FAILED_TO_FIND_DATA_FILE, "Failed to find file in local file cache">,
    EnumStringPair<FileCacheErrorType::FILE_PATH_IS_DIRECTORY, "Provided file path points to a directory">,
    EnumStringPair<FileCacheErrorType::DIRECTORY_DOES_NOT_EXIST, "Provided Directory Does Not Exist">,
    EnumStringPair<FileCacheErrorType::DATA_DIRECTORY_LOAD_FAILED, "Failed to load directory from remote storage">,
    EnumStringPair<FileCacheErrorType::DATA_DIRECTORY_SAVE_FAILED, "Failed to save directory to remote storage">,
    EnumStringPair<FileCacheErrorType::FAILED_TO_FIND_DATA_DIRECTORY, "Failed to find directory in local file cache">,
    EnumStringPair<FileCacheErrorType::DIRECTORY_PATH_IS_FILE, "Provided directory path points to a file">>;

class FileCacheError {
public:
    explicit FileCacheError(FileCacheErrorType type)
        : _type(type)
    {
    }

    explicit FileCacheError(FileCacheErrorType type, S3::S3ClientError s3Error)
        : _type(type),
        _s3Error(s3Error)
    {
    }

    [[nodiscard]] FileCacheErrorType getType() const { return _type; }
    [[nodiscard]] S3::S3ClientError getS3Error() const { return _s3Error; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<FileCacheError> result(FileCacheErrorType type) {
        return BadResult<FileCacheError>(FileCacheError(type));
    }

    template <typename... T>
    static BadResult<FileCacheError> result(FileCacheErrorType type, S3::S3ClientError s3Error) {
        return BadResult<FileCacheError>(FileCacheError(type, s3Error));
    }

private:
    FileCacheErrorType _type {};
    S3::S3ClientError _s3Error;
};

template <typename T>
using FileCacheResult = BasicResult<T, class FileCacheError>;

}
