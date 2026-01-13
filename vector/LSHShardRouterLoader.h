#pragma once

#include "FileReader.h"
#include "VectorResult.h"

namespace vec {

class LSHShardRouter;

class LSHShardRouterLoader {
public:
    LSHShardRouterLoader();
    ~LSHShardRouterLoader();

    LSHShardRouterLoader(const LSHShardRouterLoader&) = delete;
    LSHShardRouterLoader(LSHShardRouterLoader&&) = delete;
    LSHShardRouterLoader& operator=(const LSHShardRouterLoader&) = delete;
    LSHShardRouterLoader& operator=(LSHShardRouterLoader&&) = delete;

    void setFile(fs::File* file) {
        _reader.setFile(file);
    }

    [[nodiscard]] VectorResult<void> load(LSHShardRouter& router);

private:
    fs::FileReader _reader;
};

}
