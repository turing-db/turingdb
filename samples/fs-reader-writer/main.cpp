#include <numeric>
#include <spdlog/fmt/bundled/core.h>

#include "File.h"
#include "FileReader.h"
#include "FileWriter.h"

#define CHECK_RES(code)                               \
    if (auto res = (code); !res) {                    \
        fmt::print("{}\n", res.error().fmtMessage()); \
        return 1;                                     \
    }

int main() {
    fs::Path p {SAMPLE_DIR "/test"};
    const auto info = p.getFileInfo();

    fmt::print("## FileInfo struct\n");
    fmt::print("- File: {}\n", p.get());

    // Open file
    auto fileRes = fs::File::createAndOpen(p);
    if (!fileRes) {
        fmt::print("{}\n", fileRes.error().fmtMessage());
        return 1;
    }

    fs::File file = std::move(fileRes.value());

    // Clear
    CHECK_RES(file.clearContent());

    // Setup reader/writer
    fs::FileReader reader;
    fs::FileWriter writer;
    reader.setFile(&file);
    writer.setFile(&file);

    // Write 4085 4086
    // [f5 f 0 0 ] [f6 f 0 0]
    writer.write(4085);
    writer.write(4086);

    // Write 0 1 2 3 4 (as uint16_t)
    // [0 0] [1 0] [2 0] [3 0] [4 0]
    std::vector<uint16_t> integers(5);
    std::iota(integers.begin(), integers.end(), 0);
    writer.write(std::span {integers});

    // Write Hello world
    std::string str = "Hello world!";
    writer.write(str);
    writer.flush();

    // Reopen file (start back at the beginning + resets info);
    CHECK_RES(file.close());
    auto reopenRes = fs::File::open(p);
    if (!reopenRes) {
        fmt::print("{}\n", reopenRes.error().fmtMessage());
        return 1;
    }

    file = reopenRes.value();

    // Store whole content of file into buffer
    reader.read();

    // Printing whole buffer
    fmt::print("- Buffer content:");
    for (auto byte : reader.getBuffer()) {
        fmt::print(" {:x}", byte);
    }

    fmt::print("\n");

    fs::ByteBufferIterator it = reader.iterateBuffer();
    fmt::print("- FirstValue: {}\n", it.get<int>());
    fmt::print("- SecondValue: {}\n", it.get<int>());
    fmt::print("- Integers:");
    for (auto v : it.get<uint16_t>(5)) {
        fmt::print(" {}", v);
    }
    fmt::print("\n");
    fmt::print("- String: {}\n", it.get<char>(str.size()));

    return 0;
}
