#include "ProcessUtils.h"

#include <string>
#include <signal.h>
#include <sstream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <spdlog/spdlog.h>
#include <regex>

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
}

bool ProcessUtils::searchProcess(const std::string& exe, std::vector<pid_t>& pids) {
    const bool isAbsolute = FileUtils::isAbsolute(exe);

    std::vector<FileUtils::Path> procList;
    if (!FileUtils::listFiles("/proc", procList)) {
        return false;
    }

    std::string procExe;
    std::error_code error;
    std::regex brokenSymlinkRegex {" \\(deleted\\)"};
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
