#pragma once

#include "FileReader.h"
#include "VectorResult.h"

namespace vec {

struct VecLibMetadata;

class VecLibMetadataLoader {
public:
    VecLibMetadataLoader();
    ~VecLibMetadataLoader();

    VecLibMetadataLoader(const VecLibMetadataLoader&) = delete;
    VecLibMetadataLoader(VecLibMetadataLoader&&) = delete;
    VecLibMetadataLoader& operator=(const VecLibMetadataLoader&) = delete;
    VecLibMetadataLoader& operator=(VecLibMetadataLoader&&) = delete;

    void setFile(fs::File* file) {
        _reader.setFile(file);
    }

    [[nodiscard]] VectorResult<void> load(VecLibMetadata& metadata);

private:
    fs::FileReader _reader;
};

}
