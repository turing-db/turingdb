#pragma once

#include <stddef.h>
#include <stdint.h>

#include <string_view>
#include <vector>

#include "Path.h"

namespace db {

// Decoded contents of an embedding Parquet file. The embedding for _nodeIDs[i]
// is _embeddings[i], a vector of _dimension EmbeddingElement values.
struct ParquetEmbeddingData {
    // Single source of truth for the embedding element type in this layer.
    // io/parquet sits below storage, so it cannot reference types::Embedding;
    // the reader derives every element-size assumption from this alias instead.
    using EmbeddingElement = float;

    std::vector<int64_t> _nodeIDs;
    std::vector<std::vector<EmbeddingElement>> _embeddings;
    size_t _dimension {0};
};

// Reads an embedding Parquet file produced for LOAD EMBEDDING. The file is
// expected to hold exactly two top-level columns:
//   - nodeIdColumn: an INT64 column carrying internal TuringDB node IDs.
//   - embeddingColumn: a FIXED_LEN_BYTE_ARRAY column whose fixed length is
//     dimension * sizeof(float), holding little-endian float32 values.
// The dimension is derived from the fixed byte width. Throws TuringException on
// a missing column, wrong physical type, or a byte width that is not a multiple
// of sizeof(float).
class ParquetEmbeddingReader {
public:
    static void read(const fs::Path& path,
                     std::string_view nodeIdColumn,
                     std::string_view embeddingColumn,
                     ParquetEmbeddingData* out);
};

}
