#pragma once

#include <string>
#include <string_view>

enum class DemonStartResult {
    Ready,
    Failed,
    Died,
};

class DemonReadiness {
public:
    DemonReadiness();
    ~DemonReadiness();

    DemonReadiness(const DemonReadiness&) = delete;
    DemonReadiness(DemonReadiness&&) = delete;
    DemonReadiness& operator=(const DemonReadiness&) = delete;
    DemonReadiness& operator=(DemonReadiness&&) = delete;

    void open();
    void keepReadEnd();
    void keepWriteEnd();

    void notifyReady();
    void notifyFailure(std::string_view error);

    DemonStartResult waitForDemon(std::string& error);

private:
    int _readSocket {-1};
    int _writeSocket {-1};

    void send(std::string_view data);
    size_t receive(char* buffer, size_t size);
    void closeReadEnd();
    void closeWriteEnd();
};
