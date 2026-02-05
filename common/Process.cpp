#include "Process.h"

#include <string.h>

#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/wait.h>

#if __APPLE__
#include <crt_externs.h>
#endif

#include <spdlog/spdlog.h>

#include "ProcessUtils.h"

namespace {

std::string makeEnvString(const std::string& name, const std::string& value) {
    return name + "=" + value;
}

char** getEnviron() {
#if __APPLE__
    return *_NSGetEnviron();
#else
    return environ;
#endif
}

}

Process::Process()
{
}

Process::Process(const std::string& cmd)
    : _cmd(cmd)
{
}

Process::~Process() {
    if (!_waited) {
        this->kill();
        this->wait();
    }
}

void Process::setCmd(const std::string& cmd) {
    _cmd = cmd;
}

void Process::addArg(const std::string& arg) {
    _args.push_back(arg);
}

void Process::addEnvVar(const std::string& var, const std::string& value) {
    _env.emplace_back(var, value);
}

void Process::updateExitCode() {
    if (!_running) {
        return;
    }

    int status = 0;
    const auto res = waitpid(_pid, &status, WNOHANG);

    // if error occured
    if (res == -1) {
        spdlog::error("Could no wait on pid: {}", _pid);

        switch (errno) {
            case ECHILD: {
                spdlog::error("Could not wait on process: child error");
                break;
            }
            case EINTR: {
                spdlog::error("Could not wait on process: EINTR");
                break;
            }
            case EINVAL: {
                spdlog::error("Could not wait on process: EINVAL");
                break;
            }
        }

        return;
    }

    // If the process is still running, don't update exit code
    if (res == 0) {
        return;
    }

    // Process exited, update exit code
    if (WIFEXITED(status)) {
        _exitCode = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        _exitCode = WTERMSIG(status);
    } else if (WIFSTOPPED(status)) {
        _exitCode = WSTOPSIG(status);
    }

    _running = false;
}

bool Process::start() {
    if (!startAsync()) {
        return false;
    }

    this->wait();

    return true;
}

bool Process::startAsync() {
    const pid_t pid = fork();
    if (pid > 0) {
        // We are in the parent
        _pid = pid;

        return true;
    } else if (pid < 0) {
        // Process could not be created
        return false;
    }

    // We are in the child process

    // Redirect stdout to /dev/null if requested
    const int devNullFd = open("/dev/null", O_RDONLY);
    if (devNullFd < 0) {
        exit(EXIT_FAILURE);
    }

    if (!_writeStdout) {
        dup2(devNullFd, STDOUT_FILENO);
        dup2(devNullFd, STDERR_FILENO);
    }

    // Close stdin if requested
    if (!_readStdin) {
        dup2(devNullFd, STDIN_FILENO);
    }

    // Create argv
    std::vector<char*> argv;
    argv.push_back(strdup(_cmd.c_str()));
    for (const std::string& arg : _args) {
        argv.push_back(strdup(arg.c_str()));
    }
    argv.push_back(NULL);

    // Create envp
    std::vector<char*> envp;

    char** currentEnv = getEnviron();

    for (char** env = currentEnv; *env != NULL; env++) {
        envp.emplace_back(*env);
    }

    for (const auto& [name, val] : _env) {
        envp.emplace_back(strdup(makeEnvString(name, val).c_str()));
    }
    envp.push_back(NULL);

    const int execRes = execve(_cmd.c_str(), argv.data(), envp.data());
    if (execRes < 0) {
        spdlog::error("Exec failed for command {}", _cmd);
        exit(EXIT_FAILURE);
    }

    return false;
}

bool Process::terminate() {
    if (_pid < 0) {
        return false;
    }
    if (!ProcessUtils::killAllChildren(_pid, SIGTERM)) {
        return false;
    }

    return true;
}

bool Process::kill() {
    if (_pid < 0) {
        return false;
    }

    if (!ProcessUtils::killAllChildren(_pid, SIGKILL)) {
        return false;
    }

    return true;
}

bool Process::wait() {
    if (_pid < 0) {
        return false;
    }

    if (!_running) {
        return true;
    }

    int status = 0;
    const auto res = waitpid(_pid, &status, 0);
    _running = false;
    _waited = true;

    if (res == -1) {
        switch (errno) {
            case ECHILD: {
                spdlog::error("Could not wait on process: child error");
                break;
            }
            case EINTR: {
                spdlog::error("Could not wait on process: EINTR");
                break;
            }
            case EINVAL: {
                spdlog::error("Could not wait on process: EINVAL");
                break;
            }
        }
        return false;
    }

    if (WIFEXITED(status)) {
        _exitCode = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        _exitCode = WTERMSIG(status);
    } else if (WIFSTOPPED(status)) {
        _exitCode = WSTOPSIG(status);
    }

    return true;
}

bool Process::isRunning() {
    updateExitCode();
    return _running;
}

int Process::getExitCode() {
    updateExitCode();
    return _exitCode;
}
