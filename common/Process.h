#pragma once

#include <string>
#include <vector>
#include <utility>
#include <sys/types.h>

class Process {
public:
    explicit Process(const std::string& cmd);
    Process();
    Process(const Process&) = delete;
    Process& operator=(Process&) = delete;
    ~Process();

    void setCmd(const std::string& cmd);
    void addArg(const std::string& arg);
    void addEnvVar(const std::string& var, const std::string& value);

    void setWriteStdout(bool enabled) { _writeStdout = enabled; }
    void setReadStdin(bool enabled) { _readStdin = enabled; }

    pid_t getPID() const { return _pid; }

    bool isRunning();
    bool isTerminated() { return !isRunning(); }

    bool start();
    bool startAsync();

    // Sends SIGTERM to child process
    bool terminate();

    // Sends SIGKILL to child process
    bool kill();

    bool wait();

    int getExitCode();

private:
    std::string _cmd;
    std::vector<std::string> _args;
    std::vector<std::pair<std::string, std::string>> _env;
    bool _writeStdout {true};
    bool _readStdin {true};
    pid_t _pid {-1};
    int _exitCode {-1};
    bool _waited {false};
    bool _running {true};

    void updateExitCode();
};
