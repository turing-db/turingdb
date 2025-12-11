#pragma once

#include "FileWriter.h"
#include "VectorResult.h"

namespace vec {

struct VecLibMetadata;

class VecLibMetadataWriter {
public:
    static constexpr size_t BUFFER_SIZE = 1024;

    VecLibMetadataWriter();
    ~VecLibMetadataWriter();

    VecLibMetadataWriter(const VecLibMetadataWriter&) = delete;
    VecLibMetadataWriter(VecLibMetadataWriter&&) = delete;
    VecLibMetadataWriter& operator=(const VecLibMetadataWriter&) = delete;
    VecLibMetadataWriter& operator=(VecLibMetadataWriter&&) = delete;

    void setFile(fs::File* file) {
        _writer.setFile(file);
    }

    [[nodiscard]] VectorResult<void> write(const VecLibMetadata& metadata);

private:
    fs::FileWriter<BUFFER_SIZE> _writer;
};

}
