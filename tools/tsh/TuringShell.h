#pragma once

#include <memory>
#include <unordered_map>
#include <string>
#include <string_view>
#include <functional>
#include <vector>

#include "TuringClient.h"
#include "DataFrame.h"

namespace tabulate {
class Table;
}

class TuringShell {
public:
    struct Command {
        using Words = std::vector<std::string>;
        std::function<void(const Words&, TuringShell&)> _func;
    };

    TuringShell();
    ~TuringShell();

    void setDBName(const std::string& dbName) { _turing.setDBName(dbName); }

    void setDebugDumpJSON(bool enabled) { _turing.setDebugDumpJSON(enabled); }

    void startLoop();

private:
    turing::TuringClient _turing;
    turing::DataFrame _df;
    std::unique_ptr<tabulate::Table> _table;
    std::unordered_map<std::string_view, Command> _localCommands;

    void processLine(std::string& line);
    std::string composePrompt();
    void displayTable();
};