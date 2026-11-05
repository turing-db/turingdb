#pragma once

#include <utility>

#include "EmbeddingsSpec.h"
#include "TypedPropertySpec.h"

namespace db {

// The hints a LOAD JSONL carries, one per WITH clause. A JSON value alone does not say
// which property type it stands for - an array is a list or an embedding, a string is
// text or an instant - so the query names the properties that are neither by default.
struct JsonlImportSpecs {
    EmbeddingsSpec _embeddings;
    TypedPropertySpec _types;

    // Takes the hints another clause of the same statement gave
    void absorb(JsonlImportSpecs&& other) {
        for (const auto& [name, dimension] : other._embeddings) {
            _embeddings.emplace(name, dimension);
        }

        for (const auto& [name, type] : other._types) {
            _types.emplace(name, type);
        }
    }
};

}
