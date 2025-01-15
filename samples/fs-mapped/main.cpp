#include "File.h"

#define CHECK_RES(code)                               \
    if (auto res = (code); !res) {                    \
        fmt::print("{}\n", res.error().fmtMessage()); \
        return 1;                                     \
    }

int main() {
    ::srand(::time(nullptr));

    // FileInfo
    {
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

        auto file = fs::File::open(p);
        if (!file) {
            fmt::print("{}\n", file.error().fmtMessage());
            return 1;
        }

        fmt::print("\n");
    }

    {
        fs::Path p {SAMPLE_DIR "/testbinary"};
        fmt::print("## Read/Write binary values to file\n");

        {
            // Create testbinary file
            auto file = fs::File::createAndOpen(p);
            if (!file) {
                fmt::print("{}\n", file.error().fmtMessage());
                return 1;
            }
            const uint64_t v = 0;
            file->write((void*)&v, sizeof(uint64_t));
        }

        {
            fmt::print("- Opening to write binary: {}\n", p.get());
            auto file = fs::File::open(p);
            if (!file) {
                fmt::print("{}\n", file.error().fmtMessage());
                return 1;
            }

            auto region = file->map(file->getInfo()._size);
            if (!region) {
                fmt::print("{}\n", region.error().fmtMessage());
                return 1;
            }

            const uint64_t value = ::rand();
            fmt::print("- Writing uint64_t value at pos 0: {:x}\n", value);
            region->write(value);

            fmt::print("- Closing file\n");

            CHECK_RES(file->close());
        }

        {
            fmt::print("- Opening to read binary: {}\n", p.get());
            auto file = fs::File::open(p);
            if (!file) {
                fmt::print("{}\n", file.error().fmtMessage());
                return 1;
            }

            auto region = file->map(file->getInfo()._size);
            if (!region) {
                fmt::print("{}\n", region.error().fmtMessage());
                return 1;
            }

            fmt::print("- Reading uint64 at pos 0: {:x}\n", region->read<uint64_t>(0));
            fmt::print("- Closing file\n");

            CHECK_RES(file->close());
        }
    }

    return 0;
}
