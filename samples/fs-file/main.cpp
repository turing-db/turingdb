#include "File.h"

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
    fmt::print("- Exists: {}\n", info.has_value());
    fmt::print("- IsFile: {}\n", info->_type == fs::FileType::File);
    fmt::print("- IsDir: {}\n", info->_type == fs::FileType::Directory);
    fmt::print("- IsReadable: {}\n", info->readable());
    fmt::print("- IsWritable: {}\n", info->writable());
    fmt::print("- Size: {}\n", info->_size);

    auto file = fs::File::open(std::move(p));
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
    CHECK_RES(file->reopen());

    std::string fileContent;
    fileContent.resize(file->getInfo()._size);

    CHECK_RES(file->read(fileContent.data(), fileContent.size()));

    fmt::print("- Content: {:?}\n", fileContent);

    return 0;
}
