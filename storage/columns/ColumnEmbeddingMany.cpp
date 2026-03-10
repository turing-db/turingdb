#include "ColumnEmbeddingMany.h"

#include "DebugDump.h"

#include <spdlog/fmt/fmt.h>

using namespace db;

void ColumnEmbeddingMany::dump(std::ostream& out) const {
    const std::string str = fmt::format("EmbeddingMany, sz={}, dim={}", size(), _dim);
    DebugDump::dumpString(out, str);
    std::string row;
    for (size_t i = 0; i < size(); i++) {
        const std::span<const float> emb = at(i);
        row.clear();
        row += "[";
        for (uint32_t j = 0; j < _dim; j++) {
            if (j > 0) row += ", ";
            row += std::to_string(emb[j]);
        }
        row += "]";
        DebugDump::dumpString(out, row);
    }
}
