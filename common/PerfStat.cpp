#include "PerfStat.h"

#include <fstream>
#include <iostream>
#include <string>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#include "BioAssert.h"

PerfStat* PerfStat::_instance = nullptr;

PerfStat::PerfStat()
{
}

void PerfStat::init(const Path& logFile) {
    if (_instance) {
        return;
    }

    _instance = new PerfStat;
    _instance->open(logFile);
}

PerfStat* PerfStat::getInstance() {
    return _instance;
}

void PerfStat::destroy() {
    if (_instance) {
        _instance->reportTotalMem();
        _instance->close();
        delete _instance;
    }

    _instance = nullptr;
}

void PerfStat::open(const Path& logFile) {
    _outStream.open(logFile);

    if (!_outStream.is_open()) {
        std::cerr << "ERROR: failed to open the log file '" << logFile
                  << "' for write.\n";
        exit(EXIT_FAILURE);
        return;
    }
}

void PerfStat::close() {
    _outStream.close();
}

void PerfStat::reportTotalMem() {
    if (!_instance) {
        return;
    }

    const auto [reserved, physical] = getMemInMegabytes();
    _outStream << '\n'
               << "Total virtual memory reserved at exit: "
               << reserved << "MB (physical: " << physical << "MB)\n";
}

PerfStat::MemInfo PerfStat::getMemInMegabytes() const {
#ifdef __APPLE__
    // macOS: use mach API to get memory info
    mach_task_basic_info_data_t info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    const kern_return_t result = task_info(mach_task_self(),
                                     MACH_TASK_BASIC_INFO,
                                     reinterpret_cast<task_info_t>(&info),
                                     &count);
    bioassert(result == KERN_SUCCESS, "Failed to get task info");

    return {
        .reserved = info.virtual_size / (1024 * 1024),
        .rss = info.resident_size / (1024 * 1024),
    };
#else
    // Linux: read from /proc/self/status
    constexpr const char* statusFileName = "/proc/self/status";
    std::ifstream statusFile(statusFileName);
    bioassert(statusFile.is_open(), "Failed to open {}", statusFileName);

    std::string str;
    size_t reserved = 0;
    while (statusFile >> str) {
        if (str == "VmSize:") {
            statusFile >> str;
            bioassert(!str.empty(), "VmSize empty");
            const size_t memKB = std::stoull(str);
            reserved = memKB / 1024;
        } else if (str == "VmRSS:") {
            statusFile >> str;
            bioassert(!str.empty(), "VmRSS empty");
            const size_t memKB = std::stoull(str);
            return {
                .reserved = reserved,
                .rss = memKB / 1024,
            };
        }
    }

    bioassert(false, "Unreachable code");
    return {};
#endif
}
