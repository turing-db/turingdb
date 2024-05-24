#pragma once

#include <filesystem>
#include <fstream>
#include <string>

class TimerStat;

class PerfStat {
public:
    using Path = std::filesystem::path;

    friend TimerStat;

    PerfStat();

    static void init(const Path& logFile);
    static PerfStat* getInstance();
    static void destroy();

private:
    std::ofstream _outStream;
    static PerfStat* _instance;

    void open(const Path& logFile);
    void close();
    void reportTotalMem();

    struct MemInfo {
        size_t reserved {0};
        size_t rss {0};
    };

    MemInfo getMemInMegabytes() const;
};
