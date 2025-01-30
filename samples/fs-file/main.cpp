#include "File.h"

#define CHECK_RES(code)                               \
    if (auto res = (code); !res) {                    \
        fmt::print("{}\n", res.error().fmtMessage()); \
        return 1;                                     \
    }

int main() {
    fs::Path p {SAMPLE_DIR "/test"};

    auto file = fs::File::createAndOpen(p);
    if (!file) {
        fmt::print("{}\n", file.error().fmtMessage());
        return 1;
    }

    if (auto res = file->clearContent(); !res) {
        fmt::print("{}\n", res.error().fmtMessage());
        return 1;
    }

    std::string hello = "Hello world!\n";

    CHECK_RES(file->write(hello.data(), hello.size()));
    CHECK_RES(file->close());

    file = fs::File::open(p);
    if (!file) {
        fmt::print("{}\n", file.error().fmtMessage());
        return 1;
    }

    std::string fileContent;
    fileContent.resize(file->getInfo()._size);

    CHECK_RES(file->read(fileContent.data(), fileContent.size()));

    return 0;
}
