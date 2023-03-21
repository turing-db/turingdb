#ifndef _COMMON_COMMAND_
#define _COMMON_COMMAND_

#include <string>
#include <filesystem>
#include <vector>

class Command {
public:
    using Path = std::filesystem::path;

    Command(const std::string& cmd);
    ~Command();

    void setWorkingDir(const Path& path);

    void addArg(const std::string& arg);

    bool run();

    int getReturnCode() const { return _returnCode; }

private:
    std::string _cmd;
    std::vector<std::string> _args;
    Path _workingDir;
    int _returnCode {-1};
};

#endif
