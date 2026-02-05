#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "BasicResult.h"
#include "EnumToString.h"

namespace S3 {

class Path;

enum class S3ClientErrorType : uint8_t {
    UNKNOWN = 0,
    ACCESS_DENIED,
    FILE_EXISTS,
    DIRECTORY_EXISTS,
    FILE_NOT_FOUND,
    FILE_SYSTEM_ERROR,
    DIRECTORY_NOT_FOUND,
    CANNOT_UPLOAD_FILE,
    CANNOT_DOWNLOAD_FILE,
    CANNOT_UPLOAD_DIRECTORY,
    CANNOT_DOWNLOAD_DIRECTORY,
    CANNOT_OPEN_FILE,
    CANNOT_LIST_KEYS,
    CANNOT_LIST_FILES,
    CANNOT_LIST_FOLDERS,
    CANNOT_CHECK_BUCKET,
    CANNOT_CREATE_DIRECTORY_MARKER,
    INVALID_DIRECTORY_NAME,
    INVALID_KEY_NAME,
    INVALID_BUCKET_NAME,
    INVALID_PREFIX_STRING,
    _SIZE,
};

using ErrorTypeDescription = EnumToString<S3ClientErrorType>::Create<
    EnumStringPair<S3ClientErrorType::UNKNOWN, "Unknown">,
    EnumStringPair<S3ClientErrorType::ACCESS_DENIED, "Could not access S3">,
    EnumStringPair<S3ClientErrorType::FILE_EXISTS, "File already exists">,
    EnumStringPair<S3ClientErrorType::DIRECTORY_EXISTS, "Directory already exists">,
    EnumStringPair<S3ClientErrorType::DIRECTORY_NOT_FOUND, "Directory could not be found at path">,
    EnumStringPair<S3ClientErrorType::FILE_NOT_FOUND, "File could not be found at path">,
    EnumStringPair<S3ClientErrorType::FILE_SYSTEM_ERROR, "Filesystem returned error">,
    EnumStringPair<S3ClientErrorType::CANNOT_UPLOAD_FILE, "Could not upload file">,
    EnumStringPair<S3ClientErrorType::CANNOT_DOWNLOAD_FILE, "Could not download file">,
    EnumStringPair<S3ClientErrorType::CANNOT_UPLOAD_DIRECTORY, "Could not upload directory">,
    EnumStringPair<S3ClientErrorType::CANNOT_DOWNLOAD_DIRECTORY, "Could not download directory">,
    EnumStringPair<S3ClientErrorType::CANNOT_OPEN_FILE, "Could not open file for read/write">,
    EnumStringPair<S3ClientErrorType::CANNOT_LIST_KEYS, "Could not list keys">,
    EnumStringPair<S3ClientErrorType::CANNOT_LIST_FILES, "Could not list files">,
    EnumStringPair<S3ClientErrorType::CANNOT_LIST_FOLDERS, "Could not list folders">,
    EnumStringPair<S3ClientErrorType::CANNOT_CHECK_BUCKET, "Could not check if bucket exists">,
    EnumStringPair<S3ClientErrorType::CANNOT_CREATE_DIRECTORY_MARKER, "Could not create directory marker">,
    EnumStringPair<S3ClientErrorType::INVALID_DIRECTORY_NAME, "Could not find objects with the appropriate directory prefix">,
    EnumStringPair<S3ClientErrorType::INVALID_KEY_NAME, "Could not find an object mapped to the provided key">,
    EnumStringPair<S3ClientErrorType::INVALID_BUCKET_NAME, "Could not find a bucket with the provided name">,
    EnumStringPair<S3ClientErrorType::INVALID_PREFIX_STRING, "The prefix string provided to the client was not valid">>;

class S3ClientError {
public:
    S3ClientError() = default;
    explicit S3ClientError(S3ClientErrorType type)
        : _type(type)
    {
    }

    [[nodiscard]] S3ClientErrorType getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<S3ClientError> result(S3ClientErrorType type) {
        return BadResult<S3ClientError>(S3ClientError(type));
    }

private:
    S3ClientErrorType _type {};
};

template <typename T>
using S3ClientResult = BasicResult<T, class S3ClientError>;

}
