#pragma once

#include "File.h"
#include "FileWriter.h"

namespace vec {

struct VecLibShard;

static constexpr size_t METADATA_BUFFER_SIZE = 1024;
using MetadataWriter = fs::FileWriter<METADATA_BUFFER_SIZE>;

struct VecLibStorage {
    fs::File _nodeIdsFile;
    fs::File _metadataFile;
    MetadataWriter _metadataWriter;

    std::vector<VecLibShard> _shards;
};

}
