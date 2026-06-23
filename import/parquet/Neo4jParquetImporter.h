#pragma once

#include <limits>
#include <vector>

#include <stddef.h>

namespace db {

class Neo4jParquetImporter {
public:
private:
    friend class ParquetNeo4jVisitor;

    static constexpr size_t INVALID_COL_IDX = std::numeric_limits<size_t>::max();

    size_t _nodeColIdx {INVALID_COL_IDX};
    size_t _lblColIdx {INVALID_COL_IDX};

    size_t _srcColIdx {INVALID_COL_IDX};
    size_t _tgtColIdx {INVALID_COL_IDX};
    size_t _edgetypeColIdx {INVALID_COL_IDX};

    std::vector<size_t> _propCols;
};

}
