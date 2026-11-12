#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "TuringTest.h"

#include "File.h"
#include "Path.h"

using namespace turing::test;

namespace {

constexpr size_t PAYLOAD_SIZE = 256 * 1024;

std::atomic<int> handledSignals {0};

void countSignal(int) {
    handledSignals.fetch_add(1);
}

void fillPayload(std::vector<uint8_t>& payload) {
    for (size_t index = 0; index < payload.size(); index++) {
        payload[index] = static_cast<uint8_t>(index % 251);
    }
}

void readExactly(int fd, uint8_t* buffer, size_t size) {
    size_t bytesRead = 0;
    while (bytesRead < size) {
        const ssize_t nbytes = ::read(fd, buffer + bytesRead, size - bytesRead);
        ASSERT_GT(nbytes, 0);
        bytesRead += static_cast<size_t>(nbytes);
    }
}

bool isSleeping(pid_t threadID) {
    std::ifstream statFile("/proc/self/task/" + std::to_string(threadID) + "/stat");
    std::string stat;
    std::getline(statFile, stat);

    const size_t commandEnd = stat.rfind(')');
    const bool hasState = commandEnd != std::string::npos && commandEnd + 2 < stat.size();

    return hasState && stat[commandEnd + 2] == 'S';
}

void interruptWhenBlocked(std::thread& thread, const std::atomic<pid_t>& threadID) {
    while (threadID == 0 || !isSleeping(threadID)) {
        std::this_thread::yield();
    }

    ::pthread_kill(thread.native_handle(), SIGUSR1);

    while (handledSignals == 0) {
        std::this_thread::yield();
    }
}

}

class FileTransferTest : public TuringTest {
protected:
    void initialize() override {
        _path = fs::Path {_outDir + "/fd"};
        handledSignals = 0;

        // No SA_RESTART: a system call the signal interrupts returns early instead of restarting
        struct sigaction action {};
        action.sa_handler = countSignal;
        sigemptyset(&action.sa_mask);
        ::sigaction(SIGUSR1, &action, &_previousAction);
    }

    void terminate() override {
        ::sigaction(SIGUSR1, &_previousAction, nullptr);
    }

    fs::Path _path;
    struct sigaction _previousAction {};
};

TEST_F(FileTransferTest, ReadContinuesAfterAShortRead) {
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_DGRAM, 0, sockets), 0);
    ASSERT_EQ(::write(sockets[1], "abcd", 4), 4);
    ASSERT_EQ(::write(sockets[1], "efgh", 4), 4);

    auto file = fs::File::fromFd(_path, sockets[0]);
    ASSERT_TRUE(file.has_value());

    std::string received(8, '\0');
    ASSERT_TRUE(file->read(received.data(), received.size()).has_value());

    EXPECT_EQ(received, "abcdefgh");

    ::close(sockets[1]);
}

TEST_F(FileTransferTest, WriteContinuesAfterAShortWrite) {
    int pipeEnds[2];
    ASSERT_EQ(::pipe(pipeEnds), 0);

    auto file = fs::File::fromFd(_path, pipeEnds[1]);
    ASSERT_TRUE(file.has_value());

    std::vector<uint8_t> payload(PAYLOAD_SIZE);
    fillPayload(payload);

    std::atomic<pid_t> writerID {0};
    bool writeSucceeded = false;
    std::thread writer([&] {
        writerID = ::gettid();
        writeSucceeded = file->write(payload.data(), payload.size()).has_value();
    });

    interruptWhenBlocked(writer, writerID);

    std::vector<uint8_t> received(PAYLOAD_SIZE);
    readExactly(pipeEnds[0], received.data(), received.size());
    writer.join();

    EXPECT_TRUE(writeSucceeded);
    EXPECT_EQ(received, payload);

    ::close(pipeEnds[0]);
}

TEST_F(FileTransferTest, WriteRetriesAnInterruptedWrite) {
    int pipeEnds[2];
    ASSERT_EQ(::pipe(pipeEnds), 0);

    ASSERT_EQ(::fcntl(pipeEnds[1], F_SETFL, O_NONBLOCK), 0);
    const std::vector<uint8_t> filler(4096, 0);
    size_t filled = 0;
    while (::write(pipeEnds[1], filler.data(), filler.size()) > 0) {
        filled += filler.size();
    }
    ASSERT_EQ(::fcntl(pipeEnds[1], F_SETFL, 0), 0);

    auto file = fs::File::fromFd(_path, pipeEnds[1]);
    ASSERT_TRUE(file.has_value());

    std::vector<uint8_t> payload(16);
    fillPayload(payload);

    std::atomic<pid_t> writerID {0};
    bool writeSucceeded = false;
    std::thread writer([&] {
        writerID = ::gettid();
        writeSucceeded = file->write(payload.data(), payload.size()).has_value();
    });

    interruptWhenBlocked(writer, writerID);

    std::vector<uint8_t> drained(filled);
    readExactly(pipeEnds[0], drained.data(), drained.size());
    writer.join();

    ASSERT_TRUE(writeSucceeded);

    std::vector<uint8_t> received(payload.size());
    readExactly(pipeEnds[0], received.data(), received.size());
    EXPECT_EQ(received, payload);

    ::close(pipeEnds[0]);
}

TEST_F(FileTransferTest, ReadRetriesAnInterruptedRead) {
    int pipeEnds[2];
    ASSERT_EQ(::pipe(pipeEnds), 0);

    auto file = fs::File::fromFd(_path, pipeEnds[0]);
    ASSERT_TRUE(file.has_value());

    std::string received(8, '\0');
    std::atomic<pid_t> readerID {0};
    bool readSucceeded = false;
    std::thread reader([&] {
        readerID = ::gettid();
        readSucceeded = file->read(received.data(), received.size()).has_value();
    });

    interruptWhenBlocked(reader, readerID);

    ASSERT_EQ(::write(pipeEnds[1], "abcdefgh", 8), 8);
    reader.join();

    EXPECT_TRUE(readSucceeded);
    EXPECT_EQ(received, "abcdefgh");

    ::close(pipeEnds[1]);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
