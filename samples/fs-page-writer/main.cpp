#include <spdlog/fmt/bundled/core.h>

#include "FilePageWriter.h"
#include "FilePageReader.h"
#include "Time.h"

#define CHECK_RES(code)                               \
    if (auto res = (code); !res) {                    \
        fmt::print("{}\n", res.error().fmtMessage()); \
        return 1;                                     \
    }

namespace {

using Integer = uint64_t;
inline constexpr size_t INTEGER_COUNT = 1024ul * 1024 * 256 + 4; // 2GB + 4B
inline constexpr size_t NBYTES = INTEGER_COUNT * sizeof(Integer);
inline constexpr size_t NPAGES = NBYTES / fs::DEFAULT_PAGE_SIZE
                               + ((NBYTES % fs::DEFAULT_PAGE_SIZE) != 0);

}

int main() {
    fs::Path p {SAMPLE_DIR "/test"};

    {
        fmt::print("- Opening file for write\n");
        auto writer = fs::FilePageWriter::open(p);
        if (!writer) {
            fmt::print("{}\n", writer.error().fmtMessage());
            return 1;
        }

        std::vector<Integer> ones;
        ones.resize(INTEGER_COUNT, 1);

        fmt::print("- Writing into buffer:\n");
        fmt::print("  * {} integers\n  * {} MiB\n  * Page size: {} MiB\n  * {} pages\n",
                   ones.size(),
                   (float)NBYTES / 1024.f / 1024.f,
                   (float)fs::DEFAULT_PAGE_SIZE / 1024.f / 1024.f,
                   NPAGES);

        auto t0 = Clock::now();
        writer->write(std::span {ones});
        auto t1 = Clock::now();
        const float duration = Seconds(t1 - t0).count();
        fmt::print("- Write time: {} s\n", duration);
        fmt::print("- Write speed: {} MiB/s\n", (float)NBYTES / duration / 1024.0f / 1024.0f);

        writer->finish();

        if (writer->errorOccured()) {
            fmt::print("{}\n", writer->error()->fmtMessage());
            return 1;
        }
    }

    fmt::print("- Opening file for read\n");
    auto reader = fs::FilePageReader::open(p);
    if (!reader) {
        fmt::print("{}\n", reader.error().fmtMessage());
        return 1;
    }

    size_t sum = 0;
    size_t bytesRead = 0;
    const auto t0 = Clock::now();

    while (!reader->reachedEnd()) {
        reader->nextPage();
        fs::AlignedBufferIterator it = reader->begin();
        auto end = it.end();

        while (it != end) {
            sum += it.get<Integer>();
        }

        if (reader->errorOccured()) {
            fmt::print("{}\n", reader->error()->fmtMessage());
            return 1;
        }

        bytesRead += reader->getBuffer().size();
    }
    const auto t1 = Clock::now();

    const float duration = Seconds(t1 - t0).count();

    fmt::print("- Sum: {}\n", sum);

    if (sum != INTEGER_COUNT) {
        fmt::print("Error: Sum is {}, should be {}\n", sum, INTEGER_COUNT);
        return 1;
    }

    fmt::print("- Read time: {} s\n", duration);
    fmt::print("- Read speed: {} MiB/s\n", (float)bytesRead / duration / 1024.0f / 1024.0f);

    return 0;
}
