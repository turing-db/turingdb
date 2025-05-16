#pragma once

#include "BasicResult.h"
#include "EnumToString.h"
#include "S3Result.h"

#include <spdlog/fmt/bundled/format.h>

namespace db {

class Path;

enum class ErrorType : uint8_t {
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

using ErrorTypeDescription = EnumToString<ErrorType>::Create<
    EnumStringPair<ErrorType::UNKNOWN, "Unknown">,
    EnumStringPair<ErrorType::LIST_LOCAL_GRAPHS_FAILED, "Failed to list graphs available in the local file cache">,
    EnumStringPair<ErrorType::LIST_GRAPHS_FAILED, "Failed to list graphs available in remote storage">,
    EnumStringPair<ErrorType::GRAPH_LOAD_FAILED, "Failed to load graph">,
    EnumStringPair<ErrorType::GRAPH_SAVE_FAILED, "Failed to save graph">,
    EnumStringPair<ErrorType::FAILED_TO_FIND_GRAPH, "Failed to find graph in local file cache">,
    EnumStringPair<ErrorType::LIST_LOCAL_DATA_FAILED, "Failed to list data available in the local file cache">,
    EnumStringPair<ErrorType::LIST_DATA_FAILED, "Failed to list data available in remote storage">,
    EnumStringPair<ErrorType::DATA_FILE_LOAD_FAILED, "Failed to load file from remote storage">,
    EnumStringPair<ErrorType::DATA_FILE_SAVE_FAILED, "Failed to save file to remote storage">,
    EnumStringPair<ErrorType::FAILED_TO_FIND_DATA_FILE, "Failed to find file in local file cache">,
    EnumStringPair<ErrorType::FILE_PATH_IS_DIRECTORY, "Provided file path points to a directory">,
    EnumStringPair<ErrorType::DIRECTORY_DOES_NOT_EXIST, "Provided Directory Does Not Exist">,
    EnumStringPair<ErrorType::DATA_DIRECTORY_LOAD_FAILED, "Failed to load directory from remote storage">,
    EnumStringPair<ErrorType::DATA_DIRECTORY_SAVE_FAILED, "Failed to save directory to remote storage">,
    EnumStringPair<ErrorType::FAILED_TO_FIND_DATA_DIRECTORY, "Failed to find directory in local file cache">,
    EnumStringPair<ErrorType::DIRECTORY_PATH_IS_FILE, "Provided directory path points to a file">>;

class Error {
public:
    explicit Error(ErrorType type)
        : _type(type)
    {
    }

    explicit Error(ErrorType type, S3::Error s3Error)
        : _type(type),
        _s3Error(s3Error)
    {
    }

    [[nodiscard]] ErrorType getType() const { return _type; }
    [[nodiscard]] S3::Error getS3Error() const { return _s3Error; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<Error> result(ErrorType type) {
        return BadResult<Error>(Error(type));
    }

    template <typename... T>
    static BadResult<Error> result(ErrorType type, S3::Error s3Error) {
        return BadResult<Error>(Error(type, s3Error));
    }

private:
    ErrorType _type {};
    S3::Error _s3Error;
};

template <typename T>
using Result = BasicResult<T, class Error>;

}
