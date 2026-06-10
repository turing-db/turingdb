#pragma once

#include <stdint.h>
#include <gtest/gtest.h>

#include "dump/DumpConfig.h"
#include "File.h"
#include "FileReader.h"
#include "Path.h"

namespace turing::test {

inline constexpr uint64_t FNV_OFFSET_BASIS = 1469598103934665603ull;
inline constexpr uint64_t FNV_PRIME = 1099511628211ull;

// FNV-1a over a dump file's bytes — used by the dump format tests to pin
// on-disk byte formats against drift. The file header is ONE_BAD_CAFE
// (uint32) followed by VERSION (uint64); VERSION is the build's HEAD commit
// timestamp and changes with every commit, so its bytes are excluded from
// the hash. Everything else is the format under test.
inline void hashDumpFileContent(const fs::Path& path, uint64_t& hash) {
    auto file = fs::File::open(path);
    ASSERT_TRUE(file);

    fs::FileReader reader;
    reader.setFile(&file.value());
    reader.read();
    ASSERT_FALSE(reader.errorOccured());

    const auto& buffer = reader.getBuffer();

    uint64_t value = FNV_OFFSET_BASIS;
    for (size_t i = 0; i < buffer.size(); i++) {
        const bool isVersionByte = i >= db::DumpConfig::SIZEOF_ONE_BAD_CAFE
                                && i < db::DumpConfig::SIZEOF_ONE_BAD_CAFE + db::DumpConfig::SIZEOF_VERSION;
        if (isVersionByte) {
            continue;
        }

        value = (value ^ buffer[i]) * FNV_PRIME;
    }

    hash = value;
}

}
