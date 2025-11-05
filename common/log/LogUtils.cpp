#include "LogUtils.h"

#include <spdlog/spdlog.h>

namespace logt {

void TuringHomeUndefined() {
    spdlog::error("The environment variable TURING_HOME is not defined, please check your setup");
}

void FileNotFound(const std::string& path) {
    spdlog::error("File {} not found", path);
}

void DirectoryDoesNotExist(const std::string& path) {
    spdlog::error("Directory {} does not exist", path);
}

void NotADirectory(const std::string& path) {
    spdlog::error("The file {} is not a directory", path);
}

void CanNotRead(const std::string& path) {
    spdlog::error("Can not open file {} for read", path);
}

void CanNotWrite(const std::string& path) {
    spdlog::error("Impossible to write file {}", path);
}

void ExecutableNotFound(const std::string& cmd) {
    spdlog::error("Executable '{}' not found", cmd);
}

void ImpossibleToRunCommand(const std::string& cmd) {
    spdlog::error("Impossible to run command {}", cmd);
}

void CanNotRemove(const std::string& path) {
    spdlog::error("Impossible to remove {}", path);
}

void CanNotCreateDir(const std::string& path) {
    spdlog::error("Can not create directory {}", path);
}

void ElapsedTime(float time, std::string_view unit) {
    spdlog::info("Elapsed time: {} {}", time, unit);
}

void LogError(const char* msg) {
    spdlog::error("{}", msg);
}

}
