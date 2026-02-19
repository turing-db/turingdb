#include "LockFile.h"

#include <sys/file.h>
#include <charconv>

#include "BioAssert.h"
#include "File.h"
#include "FileReader.h"
#include "FileWriter.h"

using namespace db;

LockFile::LockFile()
{
}

LockFile::~LockFile() {
    unlock();
}

void LockFile::setPath(const fs::Path& p) {
    bioassert(!_locked, "Lock file is already locked");
    _path = p;
}

LockFileResult<void> LockFile::tryLock() {
    bioassert(!_path.empty(), "Path is empty");

    _locked = false;

    const int lock_fd = ::open(_path.c_str(), O_CREAT | O_RDWR, 0644);

    if (lock_fd < 0) {
        return LockFileError::result(LockFileErrorType::PERMISSION_DENIED);
    }

    if (auto res = fs::File::fromFd(_path, lock_fd); !res) {
        return LockFileError::result(LockFileErrorType::UNKNOWN);
    } else {
        _file = std::move(res.value());
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

    _file = std::move(f.value());
    _file.clearContent();

    fs::FileWriter writer;
    writer.setFile(&_file);

    writer.write(std::to_string(pid));
    writer.write('\n');
    writer.flush();

    if (writer.errorOccured()) {
        return LockFileError::result(LockFileErrorType::PERMISSION_DENIED);
    }

    return {};
}

void LockFile::unlock() {
    if (!_locked) {
        return;
    }

    _file.close();
    _path.rm();
    _locked = false;
}
