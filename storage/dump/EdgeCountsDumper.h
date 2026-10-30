#pragma once

#include "DumpResult.h"

namespace fs {
class FilePageWriter;
}

namespace db {

class EdgeCounts;

class EdgeCountsDumper {
public:
    explicit EdgeCountsDumper(fs::FilePageWriter& writer);
    ~EdgeCountsDumper();

    [[nodiscard]] DumpResult<void> dump(const EdgeCounts& counts);

private:
    fs::FilePageWriter& _writer;
};

}
