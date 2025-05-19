#pragma once

#include <unordered_map>
#include <string>
#include <string_view>
#include <functional>
#include <vector>

#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class TuringDB;
class LocalMemory;

class TuringShell {
public:
    struct Command {
        using Words = std::vector<std::string>;
        std::function<void(const Words&, TuringShell&, std::string& line)> _func;
    };

    TuringShell(TuringDB& turingDB, LocalMemory* mem);
    ~TuringShell();

    bool setGraphName(const std::string& graphName);
    bool setCommitHash(CommitHash hash);
    bool setChangeID(ChangeID changeID);
    void setQuiet(bool quiet) { _quiet = quiet; }
    void startLoop();

    void printHelp() const;

    [[nodiscard]] CommitHash getCommitHash() const { return _hash; }
    [[nodiscard]] ChangeID getChangeID() const { return _changeID; }

private:
    TuringDB& _turingDB;
    LocalMemory* _mem {nullptr};
    std::string _graphName {"default"};
    CommitHash _hash {CommitHash::head()};
    ChangeID _changeID {ChangeID::head()};
    bool _quiet {false};
    std::unordered_map<std::string_view, Command> _localCommands;

    void processLine(std::string& line);
    std::string composePrompt();
    void checkShellContext();
};

}
