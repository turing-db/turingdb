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
    void setLogFile(const Path& path);
    void setScriptPath(const Path& path);
    void setGenerateScript(bool enable) { _generateScript = enable; }
    void setWriteLogFile(bool enable) { _writeLogFile = enable; }

    void addArg(const std::string& arg);

    bool run();

    int getReturnCode() const { return _returnCode; }

private:
    std::string _cmd;
    std::vector<std::string> _args;
    Path _workingDir;
    Path _logFile;
    Path _scriptPath;
    bool _generateScript {true};
    bool _writeLogFile {true};
    int _returnCode {-1};

    bool searchCmd();
    void generateCmdString(std::string& cmdStr);
};

#endif
