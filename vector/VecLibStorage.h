#pragma once

#include "File.h"
#include "VecLibMetadataWriter.h"
#include "LSHShardRouterWriter.h"

namespace vec {

struct VecLibShard;

struct VecLibStorage {
    fs::File _metadataFile;
    fs::File _shardRouterFile;
    VecLibMetadataWriter _metadataWriter;
    LSHShardRouterWriter _shardRouterWriter;
};

}
