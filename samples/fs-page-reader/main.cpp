#include <numeric>
#include <spdlog/fmt/bundled/core.h>

#include "File.h"
#include "FileWriter.h"
#include "FilePageReader.h"
#include "Time.h"

#define CHECK_RES(code)                               \
    if (auto res = (code); !res) {                    \
        fmt::print("{}\n", res.error().fmtMessage()); \
        return 1;                                     \
    }

namespace {

using Integer = uint64_t;
inline constexpr size_t INTEGER_COUNT = 1024ul * 1024 * 128 + 7; // 1GB + 7B
inline constexpr size_t SUM = INTEGER_COUNT * (INTEGER_COUNT + 1) / 2 - INTEGER_COUNT;

int writeFileContent(const fs::Path& path) {
    // Open file
    auto file = fs::File::createAndOpen(path);
    if (!file) {
        fmt::print("{}\n", file.error().fmtMessage());
        return 1;
    }

    // Clear
    CHECK_RES(file->clearContent());

    // Setup writer
    fs::FileWriter writer;
    writer.setFile(&file.value());

    // Write 4085 4086
    // [f5 f 0 0 ] [f6 f 0 0]
    writer.write(4085);
    writer.write(4086);

    std::vector<Integer> integers(INTEGER_COUNT);
    std::iota(integers.begin(), integers.end(), 0);
    writer.write(std::span {integers});
    writer.flush();

    file->close();

    return 0;
}

}

int main() {
    fs::Path p {SAMPLE_DIR "/test"};

    if (int res = writeFileContent(p); res != 0) {
        return res;
    }

    fmt::print("- Opening file for read\n");
    auto reader = fs::FilePageReader::open(p);
    if (!reader) {
        fmt::print("{}\n", reader.error().fmtMessage());
        return 1;
    }

    fmt::print("- Reading page\n");
    if (auto res = reader->nextPage(); !res) {
        fmt::print("{}\n", res.error().fmtMessage());
        return 1;
    }

    fmt::print("- Iterating buffer\n");
    fs::AlignedBufferIterator it = reader->begin();
    fs::AlignedBufferIterator end = reader->end();

    fmt::print("- FirstValue: {}\n", it.get<int>());
    fmt::print("- SecondValue: {}\n", it.get<int>());
    fmt::print("- First 10 Integers:");
    size_t sum = 0;

    for (auto v : it.get<Integer>(10)) {
        fmt::print(" {}", v);
        sum += v;
    }

    fmt::print("\n");

    size_t bytesRead = 0;
    const auto t0 = Clock::now();

    for (;;) {
        while (it != end) {
            sum += it.get<Integer>();
        }

        bytesRead += reader->getBuffer().size();

        if (reader->reachedEnd()) {
            break;
        }

        reader->nextPage();
        it = reader->begin();
        end = reader->end();

        if (reader->errorOccured()) {
            fmt::print("{}\n", reader.error().fmtMessage());
            return 1;
        }
    }
    const auto t1 = Clock::now();

    const float duration = Seconds(t1 - t0).count();

    fmt::print("- Sum: {}\n", sum);
    if (sum != SUM) {
        fmt::print("Error: Sum is {}, should be {}\n", sum, SUM);
        return 1;
    }

    fmt::print("- Elapsed time: {} s\n", duration);
    fmt::print("- Read speed: {} MiB/s\n", (float)bytesRead / duration / 1024.0f / 1024.0f);

    return 0;
}
