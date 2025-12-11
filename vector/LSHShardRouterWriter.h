#pragma once

#include "FileWriter.h"
#include "VectorResult.h"

namespace vec {

struct LSHShardRouter;

class LSHShardRouterWriter {
public:
    static constexpr size_t BUFFER_SIZE = 1024;

    LSHShardRouterWriter();
    ~LSHShardRouterWriter();

    LSHShardRouterWriter(const LSHShardRouterWriter&) = delete;
    LSHShardRouterWriter(LSHShardRouterWriter&&) = delete;
    LSHShardRouterWriter& operator=(const LSHShardRouterWriter&) = delete;
    LSHShardRouterWriter& operator=(LSHShardRouterWriter&&) = delete;

    void setFile(fs::File* file) {
        _writer.setFile(file);
    }

    [[nodiscard]] VectorResult<void> write(const LSHShardRouter& router);

private:
    fs::FileWriter<BUFFER_SIZE> _writer;
};

}
