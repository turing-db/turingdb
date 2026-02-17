#include "LockFile.h"

#include <sys/file.h>
#include <charconv>

#include "BioAssert.h"
#include "File.h"
#include "FileReader.h"
#include "FileWriter.h"

using namespace db;

LockFile::LockFile(const fs::Path& p)
    : _path(p) {
}

LockFile::~LockFile() = default;

LockFileResult<void> LockFile::tryLock() {
    _locked = false;

    const int lock_fd = ::open(_path.c_str(), O_CREAT | O_RDWR, 0644);

    if (lock_fd < 0) {
        return LockFileError::result(LockFileErrorType::PERMISSION_DENIED);
    }

    const int lock_res = flock(lock_fd, LOCK_EX | LOCK_NB);
    if (lock_res != 0) {
        if (errno == EWOULDBLOCK) {
            // Lock is held by another process
            // Read the lock file to get PID

            auto pid = getPid();
            if (!pid) {
                return pid.get_unexpected();
            }

            return LockFileError::result(LockFileErrorType::ALREADY_LOCKED, fmt::format("by process {}", *pid));
        }

        return LockFileError::result(LockFileErrorType::UNKNOWN);
    }

    _locked = true;

    return writeMetadata();
}

LockFileResult<uint64_t> LockFile::getPid() {
    fs::Result<fs::File> f = fs::File::open(_path);
    if (!f) {
        return LockFileError::result(LockFileErrorType::PERMISSION_DENIED);
    }

    std::array<char, 256> buf {};
    f->read(buf.data(), buf.size());

    size_t endline = 0;
    for (size_t i = 0; i < buf.size(); i++) {
        if (buf[i] == '\n') {
            endline = i;
            break;
        }
    }

    const std::string_view pidStr = std::string_view {buf.data(), endline};
    if (pidStr.empty()) {
        return LockFileError::result(LockFileErrorType::NO_PID);
    }

    uint64_t id {0};
    const auto res = std::from_chars(pidStr.begin(), pidStr.end(), id);

    if (res.ec == std::errc::result_out_of_range
        || res.ec == std::errc::invalid_argument
        || (size_t)std::distance(pidStr.begin(), res.ptr) != pidStr.size()) {
        return LockFileError::result(LockFileErrorType::NO_PID);
    }

    return id;
}

LockFileResult<void> LockFile::writeMetadata() {
    bioassert(_locked, "Lock file is not locked");

    const int pid = ::getpid();

    fs::Result<fs::File> f = fs::File::open(_path);
    if (!f) {
        return LockFileError::result(LockFileErrorType::PERMISSION_DENIED);
    }

    fs::FileWriter writer;
    writer.setFile(&f.value());

    writer.write(std::to_string(pid));
    writer.write('\n');
    writer.flush();

    if (writer.errorOccured()) {
        return LockFileError::result(LockFileErrorType::PERMISSION_DENIED);
    }

    return {};
}
