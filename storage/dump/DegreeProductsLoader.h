#pragma once

#include "DumpResult.h"

namespace fs {
class FilePageReader;
}

namespace db {

class DegreeProducts;
class GraphMetadata;

class DegreeProductsLoader {
public:
    explicit DegreeProductsLoader(fs::FilePageReader& reader);
    ~DegreeProductsLoader();

    [[nodiscard]] DumpResult<void> load(const GraphMetadata& metadata, DegreeProducts& products);

private:
    fs::FilePageReader& _reader;
};

}
