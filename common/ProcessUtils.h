#pragma once

#include <sys/types.h>
#include <vector>

#include "FileUtils.h"

class ProcessUtils {
public:
    static bool killAllChildren(pid_t pid, int signal);
    static bool getAllChildren(pid_t pid, std::vector<pid_t>& children);
    static bool writePIDFile(const FileUtils::Path& file);
    static bool isProcessRunning(pid_t pid);

    ProcessUtils() = delete;
};
