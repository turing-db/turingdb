#include "ProcessUtils.h"

#include <regex>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifdef __APPLE__
#include <libproc.h>
#include <sys/proc_info.h>
#endif

#include <spdlog/spdlog.h>

#include "StringToNumber.h"
#include "FileUtils.h"

bool ProcessUtils::killAllChildren(pid_t pid, int signal) {
    std::vector<pid_t> children;
    if (!getAllChildren(pid, children)) {
        return false;
    }

    for (auto child : children) {
        kill(child, signal);
    }

    return true;
}

bool ProcessUtils::getAllChildren(pid_t pid, std::vector<pid_t>& children) {
#ifdef __APPLE__
    // macOS: Use libproc to get child processes
    const int count = proc_listchildpids(pid, nullptr, 0);
    if (count < 0) {
        return false;
    }
    if (count == 0) {
        return true;  // No children
    }

    std::vector<pid_t> childPids(count);
    const int countBytes = proc_listchildpids(pid, childPids.data(), count * sizeof(pid_t));
    if (countBytes < 0) {
        return false;
    }

    const int numChildren = countBytes / sizeof(pid_t);
    for (int i = 0; i < numChildren; i++) {
        getAllChildren(childPids[i], children);
        children.push_back(childPids[i]);
    }

    return true;
#else
    // Linux: Use /proc filesystem
    const std::string pidStr = std::to_string(pid);
    const auto procChildrenFile = "/proc/"+pidStr+"/task/"+pidStr+"/children";
    std::string childrenFileStr;

    if (!FileUtils::readContent(procChildrenFile, childrenFileStr)) {
        return false;
    }

    std::istringstream sstream(childrenFileStr);
    std::string childStr;
    while (sstream >> childStr) {
        bool convError = false;
        const pid_t childPID = StringToNumber<pid_t>(childStr, convError);
        if (convError) {
            return false;
        }

        getAllChildren(childPID, children);
        children.push_back(childPID);
    }

    return true;
#endif
}

bool ProcessUtils::searchProcess(const std::string& exe, std::vector<pid_t>& pids) {
#ifdef __APPLE__
    // macOS: Use libproc to list all processes and get their paths
    const int numPids = proc_listallpids(nullptr, 0);
    if (numPids <= 0) {
        return false;
    }

    std::vector<pid_t> allPids(numPids);
    const int numPidsActual = proc_listallpids(allPids.data(), numPids * sizeof(pid_t));
    if (numPidsActual <= 0) {
        return false;
    }

    const bool isAbsolute = (exe.length() > 0 && exe[0] == '/');
    char pathBuf[PROC_PIDPATHINFO_MAXSIZE];

    for (int i = 0; i < numPidsActual; i++) {
        const pid_t pid = allPids[i];
        if (pid == 0) continue;

        const int ret = proc_pidpath(pid, pathBuf, sizeof(pathBuf));
        if (ret <= 0) {
            continue;
        }

        std::string procPath(pathBuf);

        if (isAbsolute) {
            if (procPath == exe) {
                pids.push_back(pid);
            }
        } else {
            // Extract filename from path
            const size_t lastSlash = procPath.rfind('/');
            const std::string procName = (lastSlash != std::string::npos)
                ? procPath.substr(lastSlash + 1)
                : procPath;
            if (procName == exe) {
                pids.push_back(pid);
            }
        }
    }

    return true;
#else
    // Linux: Use /proc filesystem
    const bool isAbsolute = FileUtils::isAbsolute(exe);

    std::vector<FileUtils::Path> procList;
    if (!FileUtils::listFiles("/proc", procList)) {
        return false;
    }

    std::string procExe;
    std::error_code error;
    const std::regex brokenSymlinkRegex {" \\(deleted\\)"};
    for (const auto& procFile : procList) {
        procExe.clear();

        bool convertError = false;
        const pid_t pid = StringToNumber<pid_t>(FileUtils::getFilename(procFile).string(),
                                                convertError);
        if (convertError) {
            continue;
        }

        if (!FileUtils::isDirectory(procFile)) {
            continue;
        }

        const auto procExeFile = procFile/"exe";
        auto procExe = std::filesystem::canonical(procExeFile, error);
        if (error) {
            // Error might be caused by broken symlink (updated binaries)
            const std::string brokenSymlink = std::filesystem::read_symlink(procExeFile, error).string();

            if (error) {
                continue;
            }

            procExe = std::regex_replace(brokenSymlink, brokenSymlinkRegex, "");
        }

        if (isAbsolute) {
            if (procExe == exe) {
                pids.push_back(pid);
            }
        } else {
            if (FileUtils::getFilename(procExe) == exe) {
                pids.push_back(pid);
            }
        }
    }

    return true;
#endif
}

void ProcessUtils::stopTool(const std::string& toolName) {
    std::vector<pid_t> pids;
    if (!ProcessUtils::searchProcess(toolName, pids)) {
        spdlog::error("Can not search system processes");
        return;
    }

    const int signum = SIGTERM;
    for (pid_t pid : pids) {
        spdlog::info("Killing process: {}", pid);
        if (kill(pid, signum) < 0) {
            spdlog::error("Failed to send signal {} to process {}. Check that you have the necessary permissions.",
                          signum, pid);
        }
        spdlog::info("Stopping {}", toolName);
    }
}
