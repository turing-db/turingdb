#include "PerfStat.h"

#include <fstream>
#include <iostream>
#include <string>

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
    std::ifstream statusFile("/proc/self/status");
    bioassert(statusFile.is_open());

    std::string str;
    size_t reserved = 0;
    while (statusFile >> str) {
        if (str == "VmSize:") {
            statusFile >> str;
            bioassert(!str.empty());
            const size_t memKB = std::stoull(str);
            reserved = memKB / 1024;
        } else if (str == "VmRSS:") {
            statusFile >> str;
            bioassert(!str.empty());
            const size_t memKB = std::stoull(str);
            return {
                .reserved = reserved,
                .rss = memKB / 1024,
            };
        }
    }

    bioassert(false);
    return {};
}
