#pragma once

#include <unordered_map>
#include <string>
#include <string_view>
#include <functional>
#include <vector>

#include "TuringClient.h"
#include "DataFrame.h"

class TuringShell {
public:
    struct Command {
        using Words = std::vector<std::string>;
        std::function<void(const Words&, TuringShell&)> _func;
    };

    TuringShell();
    ~TuringShell();

    void setPort(unsigned portNum) { _turing.setPort(portNum); }
    void setAddress(const std::string& address) { _turing.setAddress(address); }

    void setGraphName(const std::string& graphName) { _turing.setGraphName(graphName); }

    void setDebugDumpJSON(bool enabled) { _turing.setDebugDumpJSON(enabled); }

    void startLoop();

    void printHelp() const;

private:
    turing::TuringClient _turing;
    turing::DataFrame _df;
    std::unordered_map<std::string_view, Command> _localCommands;

    void processLine(std::string& line);
    std::string composePrompt();
    void displayTable();
};
