#include "VecLibIdentifier.h"

#include <spdlog/fmt/bundled/format.h>
#include <cinttypes>

#include "RandomGenerator.h"

#include "VectorException.h"

using namespace vec;

VecLibIdentifier VecLibIdentifier::alloc(std::string_view baseName) {
    if (baseName.size() > MAX_BASENAME_SIZE) {
        throw VectorException("VecLibID: baseName too long");
    }

    VecLibIdentifier id;

    uint64_t randId = RandomGenerator::generate();

    // Format is: {randId:x}-{baseName} with null terminator
    const size_t size = RAND_ID_SIZE + baseName.size() + 2;
    id._data = new char[size];

    const int writtenCount = std::snprintf(id._data, size, "%016" PRIx64 "-%.*s",
                                  randId,
                                  static_cast<int>(baseName.size()),
                                  baseName.data());

    if (writtenCount < 0) {
        throw VectorException("VecLibID: snprintf failed");
    }

    id._size = static_cast<size_t>(writtenCount);

    return id;
}
