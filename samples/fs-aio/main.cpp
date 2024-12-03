#include <spdlog/fmt/bundled/core.h>

#include "File.h"
#include "Time.h"
#include "AIOEngine.h"

int main() {
    ::srand(::time(nullptr));
    auto engineRes = fs::AIOEngine::create();
    if (!engineRes) {
        fmt::print("{}\n", engineRes.error().getMessage());
        return 1;
    }

    std::unique_ptr<fs::AIOEngine> engine = std::move(engineRes.value());

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

        std::string content;
        auto file = fs::IFile::open(std::move(p));
        if (!file) {
            fmt::print("{}\n", file.error().getMessage());
            return 1;
        }

        content.resize(info->_size);

        auto t0 = Clock::now();
        engine->submitRead(file.value(), content);
        engine->wait();
        const float d = duration<Seconds>(t0, Clock::now());
        fmt::print("- Read content in {} s\n", d);
        fmt::print("- Bandwith {} MiB/s\n", (float)info->_size / 1024.f / 1024.f / d );
        fmt::print("\n");
    }

    // // Read mapped file
    // {
    //     fs::Path p {SAMPLE_DIR "/test"};
    //     fmt::print("## Read mapped file\n");

    //     {
    //         fmt::print("- Opening file for input: {}\n", p.get());
    //         auto file = fs::IFile::open(std::move(p));
    //         if (!file) {
    //             fmt::print("{}\n", file.error().getMessage());
    //             return 1;
    //         }

    //         auto region = file->map();
    //         if (!region) {
    //             fmt::print("{}\n", region.error().getMessage());
    //             return 1;
    //         }

    //         fmt::print("- File content before: {:?}\n", region->view());
    //         fmt::print("- Closing file\n");

    //         if (auto res = file->close(); !res) {
    //             fmt::print("{}\n", res.error().getMessage());
    //             return 1;
    //         }
    //     }
    //     fmt::print("\n");
    // }

    // {
    //     fs::Path p {SAMPLE_DIR "/testbinary"};
    //     fmt::print("## Read/Write binary values to file\n");

    //     {
    //         fmt::print("- Opening to write binary: {}\n", p.get());
    //         auto file = fs::IOFile::open(fs::Path(p));
    //         if (!file) {
    //             fmt::print("{}\n", file.error().getMessage());
    //             return 1;
    //         }

    //         auto region = file->map();
    //         if (!region) {
    //             fmt::print("{}\n", region.error().getMessage());
    //             return 1;
    //         }

    //         const uint64_t value = ::rand();
    //         fmt::print("- Writing uint64_t value at pos 0: {:x}\n", value);
    //         region->write<uint64_t>(value);

    //         fmt::print("- Closing file\n");

    //         if (auto res = file->close(); !res) {
    //             fmt::print("{}\n", res.error().getMessage());
    //             return 1;
    //         }
    //     }

    //     {
    //         fmt::print("- Opening to read binary: {}\n", p.get());
    //         auto file = fs::IFile::open(fs::Path(p));
    //         if (!file) {
    //             fmt::print("{}\n", file.error().getMessage());
    //             return 1;
    //         }

    //         auto region = file->map();
    //         if (!region) {
    //             fmt::print("{}\n", region.error().getMessage());
    //             return 1;
    //         }

    //         fmt::print("- Reading uint64 at pos 0: {:x}\n", region->read<uint64_t>(0));
    //         fmt::print("- Closing file\n");

    //         if (auto res = file->close(); !res) {
    //             fmt::print("{}\n", res.error().getMessage());
    //             return 1;
    //         }
    //     }
    // }

    return 0;
}
