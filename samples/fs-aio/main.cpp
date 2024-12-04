#include <fcntl.h>
#include <numeric>
#include <spdlog/fmt/bundled/core.h>
#include <sys/stat.h>

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
    std::string content;

    // Write for the read test
    {
        fs::Path p {SAMPLE_DIR "/testread"};

        constexpr size_t charCount = 30000000;
        content.resize(charCount);
        std::iota(content.begin(), content.end(), '0');

        int fd = open(p.get().c_str(), O_WRONLY | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            fmt::print("Could not open {} for write\n", p.get());
            return 1;
        }

        const auto t0 = Clock::now();
        ssize_t bytesWritten = write(fd, content.c_str(), content.size());
        if (bytesWritten < 0 || bytesWritten < (ssize_t)content.size()) {
            close(fd);
            fmt::print("Could not write\n", p.get());
            return 1;
        }

        const size_t mib = (float)charCount / 1024.f / 1024.f;
        const float d = duration<Seconds>(t0, Clock::now());
        fmt::print("## Write with ::write()\n");
        fmt::print("- Wrote {} MiB in {} s\n", mib, d);
        fmt::print("- Bandwith {} MiB/s\n", mib / d);
        fmt::print("\n");

        close(fd);
        content.resize(0);
    }

    // Read
    {
        fs::Path p {SAMPLE_DIR "/testread"};
        const auto info = p.getFileInfo();

        fmt::print("## Read\n");
        fmt::print("- File: {}\n", p.get());

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
        const size_t mib = (float)info->_size / 1024.f / 1024.f;
        fmt::print("- Read {} MiB in {} s\n", mib, d);
        fmt::print("- Bandwith {} MiB/s\n", mib / d);
        fmt::print("\n");
        content.resize(0);
    }

    // Write
    {
        fs::Path p {SAMPLE_DIR "/testwrite"};
        constexpr size_t charCount = 30000000;

        fmt::print("## Write\n");
        fmt::print("- File: {}\n", p.get());

        auto file = fs::IOFile::open(std::move(p));
        if (!file) {
            fmt::print("[Open]: {}\n", file.error().getMessage());
            return 1;
        }

        content.resize(charCount);
        std::iota(content.begin(), content.end(), '0');

        auto t0 = Clock::now();
        engine->submitWrite(file.value(), content);
        engine->wait();
        const size_t mib = (float)charCount / 1024.f / 1024.f;
        const float d = duration<Seconds>(t0, Clock::now());
        fmt::print("- Wrote {} MiB in {} s\n", mib, d);
        fmt::print("- Bandwith {} MiB/s\n", mib / d);
        fmt::print("\n");
        content.resize(0);
    }

    return 0;
}
