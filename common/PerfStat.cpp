#include "PerfStat.h"

#include <iostream>
#include <fstream>
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

    const size_t memAmount = getReservedMemInMegabytes();
    _outStream << '\n' << "Total virtual memory reserved at exit: "
               << memAmount << "MB\n";
}

size_t PerfStat::getReservedMemInMegabytes() {
    std::ifstream statusFile("/proc/self/status");
    bioassert(statusFile.is_open());

    std::string str;
    while (statusFile >> str) {
        if (str == "VmSize:") {
            statusFile >> str;
            bioassert(!str.empty());
            const size_t memKB = std::stoull(str);
            return memKB/1024;
        }
    }

    bioassert(false);
    return 0;
}
