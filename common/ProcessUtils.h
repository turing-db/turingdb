#pragma once

#include <sys/types.h>
#include <vector>

class ProcessUtils {
public:
    static bool killAllChildren(pid_t pid, int signal);
    static bool getAllChildren(pid_t pid, std::vector<pid_t>& children);

    ProcessUtils() = delete;
};
