#pragma once

#include <optional>

#include "BasicResult.h"
#include "EnumToString.h"
#include "FileResult.h"

namespace vec {

enum class VectorErrorCode : uint8_t {
    Unknown = 0,

    InvalidStorageRoot,
    EmptyLibName,
    NotVecLibFile,
    InvalidDimension,
    InvalidMetric,
    InvalidMetadata,
    ReaderNotInitialized,
    WriterNotInitialized,

    LibraryDoesNotExist,
    LibraryAlreadyExists,
    LibraryAlreadyLoaded,
    LibraryStorageCorrupted,
    CouldNotListLibraries,

    CouldNotCreateStorage,
    CouldNotCreateLibraryStorage,
    CouldNotCreateShardRouterStorage,
    CouldNotCreateMetadataStorage,
    CouldNotCreateExternalIDsStorage,
    CouldNotCreateEmbeddingsStorage,
    LibraryStorageAlreadyExists,
    LibraryStorageAlreadyLoaded,

    CouldNotOpenShardRouterFile,
    CouldNotOpenMetadataFile,
    CouldNotWriteShardRouterFile,
    CouldNotWriteMetadataFile,
    CouldNotClearShardRouterFile,
    CouldNotClearMetadataFile,

    CouldNotOpenExternalIDsFile,
    CouldNotWriteExternalIDsFile,
    CouldNotClearExternalIDsFile,
    CouldNotSeekBeginningOfExternalIDsFile,
    CouldNotReadExternalIDsFile,
    ExternalIDsFileInvalid,

    CouldNotLoadShardRouterFile,
    ShardRouterFileEmpty,
    ShardRouterInvalidDimension,
    ShardRouterInvalidBitCount,
    ShardRouterInvalidVectors,

    CouldNotLoadMetadataFile,
    CouldNotLoadNodeIdsFile,

    _SIZE,
};

using VectorErrorTypeDescription = EnumToString<VectorErrorCode>::Create<
    EnumStringPair<VectorErrorCode::Unknown, "Unknown">,

    EnumStringPair<VectorErrorCode::InvalidStorageRoot, "Invalid storage root path">,
    EnumStringPair<VectorErrorCode::EmptyLibName, "Empty library name">,
    EnumStringPair<VectorErrorCode::NotVecLibFile, "Not a vectorlib file">,
    EnumStringPair<VectorErrorCode::InvalidDimension, "Invalid dimension">,
    EnumStringPair<VectorErrorCode::InvalidMetric, "Invalid metric">,
    EnumStringPair<VectorErrorCode::InvalidMetadata, "Invalid metadata file">,
    EnumStringPair<VectorErrorCode::ReaderNotInitialized, "Reader not initialized">,
    EnumStringPair<VectorErrorCode::WriterNotInitialized, "Writer not initialized">,

    EnumStringPair<VectorErrorCode::LibraryDoesNotExist, "Library does not exist">,
    EnumStringPair<VectorErrorCode::LibraryAlreadyExists, "Library already exists">,
    EnumStringPair<VectorErrorCode::LibraryAlreadyLoaded, "Library already loaded: ">,
    EnumStringPair<VectorErrorCode::LibraryStorageCorrupted, "Library storage corrupted">,
    EnumStringPair<VectorErrorCode::CouldNotListLibraries, "Could not list libraries on storage root path">,

    EnumStringPair<VectorErrorCode::CouldNotCreateStorage, "Could not create storage">,
    EnumStringPair<VectorErrorCode::CouldNotCreateLibraryStorage, "Could not create library storage">,
    EnumStringPair<VectorErrorCode::CouldNotCreateShardRouterStorage, "Could not create shard router storage">,
    EnumStringPair<VectorErrorCode::CouldNotCreateMetadataStorage, "Could not create metadata storage">,
    EnumStringPair<VectorErrorCode::CouldNotCreateExternalIDsStorage, "Could not create node IDs storage">,
    EnumStringPair<VectorErrorCode::CouldNotCreateEmbeddingsStorage, "Could not create embeddings storage">,
    EnumStringPair<VectorErrorCode::LibraryStorageAlreadyExists, "Library storage already exists">,
    EnumStringPair<VectorErrorCode::LibraryStorageAlreadyLoaded, "Library storage already loaded">,

    EnumStringPair<VectorErrorCode::CouldNotOpenShardRouterFile, "Could not open shard router file">,
    EnumStringPair<VectorErrorCode::CouldNotOpenMetadataFile, "Could not open metadata file">,
    EnumStringPair<VectorErrorCode::CouldNotWriteShardRouterFile, "Could not write shard router file">,
    EnumStringPair<VectorErrorCode::CouldNotWriteMetadataFile, "Could not write metadata file">,
    EnumStringPair<VectorErrorCode::CouldNotClearShardRouterFile, "Could not clear shard router file">,
    EnumStringPair<VectorErrorCode::CouldNotClearMetadataFile, "Could not clear metadata file">,

    EnumStringPair<VectorErrorCode::CouldNotOpenExternalIDsFile, "Could not open node IDs file">,
    EnumStringPair<VectorErrorCode::CouldNotWriteExternalIDsFile, "Could not write node IDs file">,
    EnumStringPair<VectorErrorCode::CouldNotClearExternalIDsFile, "Could not clear node IDs file">,
    EnumStringPair<VectorErrorCode::CouldNotSeekBeginningOfExternalIDsFile, "Could not seek beginning of node IDs file">,
    EnumStringPair<VectorErrorCode::CouldNotReadExternalIDsFile, "Could not read node IDs file">,
    EnumStringPair<VectorErrorCode::ExternalIDsFileInvalid, "Node IDs file has invalid format">,

    EnumStringPair<VectorErrorCode::CouldNotLoadShardRouterFile, "Could not load shard router file">,
    EnumStringPair<VectorErrorCode::ShardRouterFileEmpty, "Shard router file is empty">,
    EnumStringPair<VectorErrorCode::ShardRouterInvalidDimension, "Shard router file has invalid dimension">,
    EnumStringPair<VectorErrorCode::ShardRouterInvalidBitCount, "Shard router file has invalid bit count">,
    EnumStringPair<VectorErrorCode::ShardRouterInvalidVectors, "Shard router file has invalid vectors">,

    EnumStringPair<VectorErrorCode::CouldNotLoadMetadataFile, "Could not load metadata file">,
    EnumStringPair<VectorErrorCode::CouldNotLoadNodeIdsFile, "Could not load node IDs file">>;

class VectorError {
public:
    explicit VectorError(VectorErrorCode type, std::optional<fs::Error> fileError = {})
        : _type(type),
        _fileError(fileError)
    {
    }

    [[nodiscard]] VectorErrorCode getType() const { return _type; }
    [[nodiscard]] std::optional<fs::Error> getFileError() const { return _fileError; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<VectorError> result(VectorErrorCode type,
                                         std::optional<fs::Error> fileError = {}) {
        return BadResult<VectorError>(VectorError(type, fileError));
    }

private:
    VectorErrorCode _type;
    std::optional<fs::Error> _fileError;
};

template <typename T>
using VectorResult = BasicResult<T, class VectorError>;

}
