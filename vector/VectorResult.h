#pragma once

#include "BasicResult.h"
#include "EnumToString.h"

namespace vec {

enum class VectorErrorCode : uint8_t {
    Unknown = 0,

    EmptyLibName,
    DoesNotExist,
    InvalidStorageRoot,
    CouldNotCreateStorage,
    CouldNotCreateLibraryStorage,
    CouldNotCreateMetadataStorage,
    CouldNotCreateNodeIdsStorage,
    CouldNotCreateEmbeddingsStorage,
    LibraryStorageAlreadyExists,
    CouldNotOpenMetadataFile,
    CouldNotWriteMetadataFile,
    CouldNotClearMetadataFile,
    NotVecLibFile,
    InvalidDimension,

    _SIZE,
};

using VectorErrorTypeDescription = EnumToString<VectorErrorCode>::Create<
    EnumStringPair<VectorErrorCode::Unknown, "Unknown">,
    EnumStringPair<VectorErrorCode::EmptyLibName, "Empty library name">,
    EnumStringPair<VectorErrorCode::DoesNotExist, "Vector does not exist">,
    EnumStringPair<VectorErrorCode::InvalidStorageRoot, "Invalid storage root path">,
    EnumStringPair<VectorErrorCode::CouldNotCreateStorage, "Could not create storage for library">,
    EnumStringPair<VectorErrorCode::CouldNotCreateLibraryStorage, "Could not create library storage for library">,
    EnumStringPair<VectorErrorCode::CouldNotCreateMetadataStorage, "Could not create metadata storage for library">,
    EnumStringPair<VectorErrorCode::CouldNotCreateNodeIdsStorage, "Could not create node IDs storage for library">,
    EnumStringPair<VectorErrorCode::CouldNotCreateEmbeddingsStorage, "Could not create embeddings storage for library">,
    EnumStringPair<VectorErrorCode::LibraryStorageAlreadyExists, "Library storage already exists for library">,
    EnumStringPair<VectorErrorCode::CouldNotOpenMetadataFile, "Could not open metadata file for library">,
    EnumStringPair<VectorErrorCode::CouldNotWriteMetadataFile, "Could not write metadata file for library">,
    EnumStringPair<VectorErrorCode::CouldNotClearMetadataFile, "Could not clear metadata file for library">,
    EnumStringPair<VectorErrorCode::NotVecLibFile, "Not a vectorlib file">,
    EnumStringPair<VectorErrorCode::InvalidDimension, "Invalid dimension">>;

class VectorError {
public:
    explicit VectorError(VectorErrorCode type)
        : _type(type)
    {
    }

    [[nodiscard]] VectorErrorCode getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<VectorError> result(VectorErrorCode type) {
        return BadResult<VectorError>(VectorError(type));
    }

private:
    VectorErrorCode _type;

};

template <typename T>
using VectorResult = BasicResult<T, class VectorError>;

}
