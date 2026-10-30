#pragma once

#include "DumpResult.h"

namespace fs {
class FilePageWriter;
}

namespace db {

class DegreeProducts;

class DegreeProductsDumper {
public:
    explicit DegreeProductsDumper(fs::FilePageWriter& writer);
    ~DegreeProductsDumper();

    [[nodiscard]] DumpResult<void> dump(const DegreeProducts& products);

private:
    fs::FilePageWriter& _writer;
};

}
