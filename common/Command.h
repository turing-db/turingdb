#pragma once

#include <string>
#include <filesystem>
#include <vector>
#include <utility>
#include <memory>

class Process;

class Command {
public:
    using Path = std::filesystem::path;

    Command(const std::string& cmd);
    ~Command();

    void setWorkingDir(const Path& path);
    void setEnvVar(const std::string& var, const std::string& value);

    void setLogFile(const Path& path);
    void setScriptPath(const Path& path);
    void setGenerateScript(bool enable) { _generateScript = enable; }
    void setWriteLogFile(bool enable) { _writeLogFile = enable; }
    void setWriteOnStdout(bool enable) { _stdoutEnabled = enable; }
    void setVerbose(bool verbose) { _verbose = verbose; }

    void addArg(const std::string& arg);
    void addOption(const std::string& optName, const std::string& arg);

    bool run();
    std::unique_ptr<Process> runAsync();

    int getReturnCode() const { return _returnCode; }
    void getLogs(std::string& data) const;

private:
    std::string _cmd;
    std::vector<std::pair<std::string, std::string>> _env;
    std::vector<std::pair<std::string, std::string>> _options;
    std::vector<std::string> _args;
    Path _workingDir;
    Path _logFile;
    Path _scriptPath;
    bool _generateScript {true};
    bool _writeLogFile {true};
    bool _stdoutEnabled {false};
    bool _verbose {false};
    int _returnCode {-1};

    bool searchCmd();
    void generateCmdString(std::string& cmdStr);
    bool generateBashCmd(Process& proc);
};
