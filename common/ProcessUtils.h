#pragma once

#include <sys/types.h>
#include <vector>
#include <string>

class ProcessUtils {
public:
    static bool killAllChildren(pid_t pid, int signal);
    static bool getAllChildren(pid_t pid, std::vector<pid_t>& children);

    // If exe is an absolute path, write in pids the processes with that executable if any
    // If exe is not a path,  write in pids the processes with that executable file name
    static bool searchProcess(const std::string& exe, std::vector<pid_t>& pids);
    static void stopTool(const std::string& toolName);

    ProcessUtils() = delete;
};
