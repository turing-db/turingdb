#include "TuringShell.h"

#include <linenoise.h>
#include <tabulate/table.hpp>
#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <termcolor/termcolor.hpp>

#include "TuringDB.h"

using namespace db;

namespace {

const char* whiteChars = " \n\r\t";

// Remove whitespace in front of a string
inline void trim(std::string& str) {
    str.erase(str.begin(), std::find_if(str.begin(), str.end(),
    [](char ch) {
        return !isspace(ch);
    }));
}

inline std::string_view getFirstWord(const std::string& str) {
    const auto pos = str.find_first_of(" \t\r\n");
    if (pos == std::string::npos) {
        return std::string_view(str.c_str(), str.size());
    }

    return std::string_view(str.c_str(), pos);
}

void extractWords(std::vector<std::string>& words, const std::string& line) {
    size_t pos = 0;
    while (pos < line.size()) {
        // Skip whitespace
        pos = line.find_first_not_of(whiteChars, pos);
        if (pos == std::string::npos) {
            // We went to the end of the line
            return;
        }

        // Get position of the end of the word
        size_t newPos = line.find_first_of(whiteChars, pos);
        if (newPos == std::string::npos) {
            newPos = line.size();
        }

        words.emplace_back(std::string(line.c_str()+pos, newPos-pos));
        pos = newPos;
    }
}

// Commands
void helpCommand(const TuringShell::Command::Words& args, TuringShell& shell) {
    shell.printHelp();
}

void quitCommand(const TuringShell::Command::Words& args, TuringShell& shell) {
    exit(EXIT_SUCCESS);
}

void changeDBCommand(const TuringShell::Command::Words& args, TuringShell& shell) {
    std::string graphName;
    argparse::ArgumentParser argParser("cd",
                               "",
                              argparse::default_arguments::help,
             false);
    argParser.add_description("Print Turing Shell help");
    argParser.add_argument("graph")
             .nargs(1)
             .metavar("graph_name")
             .store_into(graphName);
    argParser.parse_args(args);

    if (graphName.empty()) {
        spdlog::error("Graph name can not be empty");
        return;
    }

    shell.setGraphName(graphName);
}

}

TuringShell::TuringShell(TuringDB& turingDB, LocalMemory* mem)
    : _turingDB(turingDB),
      _mem(mem)
{
    _localCommands.emplace("q", Command{quitCommand});
    _localCommands.emplace("quit", Command{quitCommand});
    _localCommands.emplace("exit", Command{quitCommand});
    _localCommands.emplace("help", Command{helpCommand});
    _localCommands.emplace("cd", Command{changeDBCommand});
}

TuringShell::~TuringShell() {
}

void TuringShell::startLoop() {
    std::string shellPrompt = composePrompt();
    char* line = NULL;
    std::string lineStr;
    while ((line = linenoise(shellPrompt.c_str())) != NULL) {
        lineStr = line;
        if (lineStr.empty()) {
            continue;
        }

        processLine(lineStr);

        linenoiseHistoryAdd(line);
        shellPrompt = composePrompt();
    }
}

std::string TuringShell::composePrompt() {
    const std::string basePrompt = "turing";
    const char* separator = ":";

    std::string prompt = basePrompt;
    prompt += separator;
    prompt += _graphName;
    prompt += "> ";
    return prompt;
}

void TuringShell::processLine(std::string& line) {
    // Remove leading whitespace
    trim(line);

    // Check if it is a local command
    const auto cmdName = getFirstWord(line);
    const auto localCmdIt = _localCommands.find(cmdName);
    if (localCmdIt != _localCommands.end()) {
        Command::Words words;
        extractWords(words, line);
        localCmdIt->second._func(words, *this);
        return;
    }

    // Execute query
    const auto res = _turingDB.query(line, _graphName, _mem);
    if (!res.isOk()) {
        if (res.hasErrorMessage()) {
            spdlog::error("{}: {}", QueryStatusDescription::value(res.getStatus()), res.getError());
        } else {
            spdlog::error("{}", QueryStatusDescription::value(res.getStatus()));
        }
        return;
    }

    std::cout << "Query executed in " << res.getTotalTime().count() << " ms.\n";
}

/*
void TuringShell::displayTable() {
    tabulate::Table table;

    for (const auto& row : _df.rows()) {
        tabulate::RowStream rs;

        for (const auto& value : row) {
            rs << value.toString();
        }

        table.add_row(std::move(rs));
    }

    std::cout << table << "\n";
}
*/

void TuringShell::printHelp() const {
    for (const auto& entry : _localCommands) {
        std::cout << entry.first << "\n";
    }

    std::cout << "\n";
}
