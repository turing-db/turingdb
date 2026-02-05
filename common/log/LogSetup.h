#pragma once

#include <string>

class LogSetup {
public:
    LogSetup() = delete;

    static void setupLogFileBacked(const std::string& path);
    static void setupLogConsole();

    static void logFlush();
};
