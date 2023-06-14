#include "PerfStat.h"
#include "BioAssert.h"

#include <iostream>

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
