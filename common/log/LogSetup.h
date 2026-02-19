#pragma once

#include <string>

class LogSetup {
public:
    LogSetup() = delete;

    static void setupLogFileBacked(const std::string& path, bool truncate = true);
    static void setupLogConsole();

    static void logFlush();
};
