#pragma once

#include <memory>

#include "TuringClient.h"
#include "DataFrame.h"
#include "BasicResult.h"

namespace tabulate {
class Table;
}

class TuringShell {
public:
    enum class CommandType {
        LOCAL_CMD,
        REMOTE_CMD
    };

    enum class CommandError {
        ARG_EXPECTED
    };

    using Result = BasicResult<CommandType, CommandError>;

    TuringShell();
    ~TuringShell();

    void setDBName(const std::string& dbName) { _turing.setDBName(dbName); }

    void startLoop();

private:
    turing::TuringClient _turing;
    turing::DataFrame _df;
    std::unique_ptr<tabulate::Table> _table;

    void process(std::string& line);
    std::string composePrompt();
    Result processCommand(const std::string& line);
    void displayTable();
};