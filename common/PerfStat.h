#pragma once

#include <fstream>
#include <filesystem>
#include <string>

class TimerStat;

class PerfStat {
public:
    using Path = std::filesystem::path;

    friend TimerStat;

    PerfStat();

    static void init(const Path& path);
    static PerfStat* getInstance();
    static void destroy();

private:
    std::ofstream _outStream;
    static PerfStat* _instance;

    void open(const Path& path);
    void close();
    void reportTotalMem();
    size_t getReservedMemInMegabytes();
};
