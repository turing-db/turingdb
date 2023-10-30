#include "ProcessUtils.h"

#include <string>
#include <signal.h>
#include <sstream>

#include "StringToNumber.h"
#include "FileUtils.h"

#include "BioLog.h"
using namespace Log;

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
