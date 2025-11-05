#include "Demonology.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>

#include <spdlog/spdlog.h>

void Demonology::demonize() {
    // We are already a demon
    if (getppid() == 1) {
        return;
    }

    // Fork from parent process
    pid_t pid = fork();
    if (pid < 0) {
        spdlog::error("Demonize error, failed to fork from parent process");
        exit(EXIT_FAILURE);
    }

    if (pid > 0) {
        // We are in the parent process, kill ourselves
        exit(EXIT_SUCCESS);
    }

    // Enter new session to escape from the controlling terminal
    const pid_t sid = setsid();
    if (sid < 0) {
        spdlog::error("Demonize error, failed to create new session");
        exit(EXIT_FAILURE);
    }

    // Fork again so we are not a session leader
    pid = fork();
    if (pid < 0) {
        spdlog::error("Demonize error, failed to fork after creating a new session");
        exit(EXIT_FAILURE);
    }

    if (pid > 0) {
        // We are in the parent process, kill ourselves
        spdlog::info("Spawned process {}", pid);
        exit(EXIT_SUCCESS);
    }

    // Change current directory to filesystem root
    if (chdir("/") < 0) {
        spdlog::error("Demonize error, failed to change current directory to /");
        exit(EXIT_FAILURE);
    }

    // Reset umask
    if (umask(027) < 0) {
        spdlog::error("Demonize error, failed to change umask");
        exit(EXIT_FAILURE);
    }

    // Open standard descriptors to /dev/null
    const int devNullFd = open("/dev/null", O_RDWR, 0);
    if (devNullFd < 0) {
        spdlog::error("Demonize error, failed to open /dev/null");
        exit(EXIT_FAILURE);
    }

    dup2(devNullFd, STDIN_FILENO);
    dup2(devNullFd, STDOUT_FILENO);
    dup2(devNullFd, STDERR_FILENO);
}
