#pragma once

#include <unordered_map>

#include "File.h"
#include "FileWriter.h"
#include "LSHSignature.h"

namespace vec {

struct VecLibShard;

static constexpr size_t METADATA_BUFFER_SIZE = 1024;
using MetadataWriter = fs::FileWriter<METADATA_BUFFER_SIZE>;

struct VecLibStorage {
    fs::File _nodeIdsFile;
    fs::File _metadataFile;
    MetadataWriter _metadataWriter;

    std::unordered_map<LSHSignature, std::unique_ptr<VecLibShard>> _shards;
};

}
