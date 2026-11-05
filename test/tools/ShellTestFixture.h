#pragma once

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <future>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include "LineNoiseHandle.h"
#include "TuringShell.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

namespace turing::test {

// A shell fed through a pipe on stdin, with HOME pointed at the test's directory
class ShellTestFixture : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        const char* home = getenv("HOME");
        _savedHome = home ? home : "";
        setenv("HOME", _outDir.c_str(), 1);

        _savedStdin = dup(STDIN_FILENO);
        ASSERT_EQ(pipe(_stdinPipe), 0);
        dup2(_stdinPipe[0], STDIN_FILENO);
        clearerr(stdin);
    }

    void terminate() override {
        dup2(_savedStdin, STDIN_FILENO);
        close(_savedStdin);
        close(_stdinPipe[0]);
        closeStdinWriter();
        clearerr(stdin);

        setenv("HOME", _savedHome.c_str(), 1);
    }

    void writeStdin(std::string_view text) {
        ASSERT_EQ(write(_stdinPipe[1], text.data(), text.size()), static_cast<ssize_t>(text.size()));
    }

    void closeStdinWriter() {
        if (_stdinPipe[1] != -1) {
            close(_stdinPipe[1]);
            _stdinPipe[1] = -1;
        }
    }

    // Whether @param loop ends within @param timeout; a loop that does not is stopped, so
    // the test ends too
    bool waitForLoop(db::TuringShell& shell, std::future<void>& loop, std::chrono::seconds timeout) {
        const bool ended = loop.wait_for(timeout) == std::future_status::ready;
        if (!ended) {
            closeStdinWriter();
            shell.stop();
            loop.wait();
        }

        return ended;
    }

    void expectLoopEnds(db::TuringShell& shell, std::future<void>& loop) {
        EXPECT_TRUE(waitForLoop(shell, loop, std::chrono::seconds(5))) << "the shell loop did not end";
    }

    // Runs @param lines through one shell whose stdin ends after them, with stdout
    // pointed at a file for the loop's duration only, so a failed assertion still
    // prints to the console; fills @param output with what the shell printed
    void runLines(std::string_view lines, std::string& output) {
        const std::string outputPath = std::string(_outDir) + "/shell.out";

        const int file = open(outputPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(file, 0);

        fflush(stdout);
        const int savedStdout = dup(STDOUT_FILENO);
        dup2(file, STDOUT_FILENO);
        close(file);

        db::TuringShell shell(_env->getDB(), &_env->getMem(), &_lineNoiseHandle);
        std::future<void> loop = std::async(std::launch::async, [&shell] { shell.startLoop(); });

        writeStdin(lines);
        closeStdinWriter();

        const bool ended = waitForLoop(shell, loop, std::chrono::seconds(10));

        fflush(stdout);
        dup2(savedStdout, STDOUT_FILENO);
        close(savedStdout);

        ASSERT_TRUE(ended) << "the shell loop did not end at the end of its input";

        std::ifstream read(outputPath);
        std::stringstream buffer;
        buffer << read.rdbuf();
        output = buffer.str();
    }

    std::unique_ptr<TuringTestEnv> _env;
    db::LineNoiseHandle _lineNoiseHandle;
    std::string _savedHome;
    int _savedStdin {-1};
    int _stdinPipe[2] {-1, -1};
};

}
