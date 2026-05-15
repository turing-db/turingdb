#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

#include "IClient.h"

namespace db {

class TuringDB;
class LocalMemory;
class LineNoiseHandle;

class TuringShell {
public:
    struct Command {
        using Words = std::vector<std::string>;
        std::function<void(const Words&, TuringShell&, std::string& line)> _func;
    };

    TuringShell(TuringDB& turingDB,
                LocalMemory* mem,
                LineNoiseHandle*);
    ~TuringShell();

    bool setGraphName(const std::string& graphName);
    bool setCommitHash(CommitHash hash);
    bool setChangeID(ChangeID changeID);
    void setQuiet(bool quiet) { _quiet = quiet; }
    void startLoop();
    void connectRemote(const std::string& address, const std::string& port);
    void disconnectRemote();
    [[nodiscard]] bool isRemoteConnected() const { return _remoteConnected; }

    void printHelp() const;
    void stop();

    [[nodiscard]] CommitHash getCommitHash() const { return _hash; }
    [[nodiscard]] ChangeID getChangeID() const { return _changeID; }
    net::IClient& getTuringClient() { return *_client; }

private:
    TuringDB& _turingDB;
    // Concrete type chosen at construction based on the USE_TURING_H2_CLIENT
    // env var: H2Client when set, TuringClient otherwise.
    std::unique_ptr<net::IClient> _client;
    LocalMemory* _mem {nullptr};
    std::string _graphName {"default"};
    CommitHash _hash {CommitHash::head()};
    ChangeID _changeID {ChangeID::head()};
    std::string _remoteAddress;
    std::string _remotePort;
    bool _remoteConnected {false};
    bool _quiet {false};
    pthread_t _threadID {};
    std::atomic<bool> _running {true};
    LineNoiseHandle* _lineNoiseHandle {nullptr};
    std::unordered_map<std::string_view, Command> _localCommands;

    void processLine(std::string& line);
    void formatMessage(std::string& msg);
    std::string composePrompt();
    void checkShellContext();
};
}
