#include "TuringShell.h"

#include <chrono>
#include <ratio>
#include <string>
#include <algorithm>
#include <ctype.h>
#include <string_view>

#include <linenoise.h>
#include <tabulate/table.hpp>

#include "LogUtils.h"
#include "TimerStat.h"

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

void extractWords(std::vector<std::string_view>& words, const std::string& line) {
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

        words.emplace_back(line.c_str()+pos, newPos-pos);
        pos = newPos;
    }
}

}

TuringShell::TuringShell()
{
    _table = std::make_unique<Table>();
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

        process(lineStr);

        linenoiseHistoryAdd(line);
        shellPrompt = composePrompt();
    }
}

std::string TuringShell::composePrompt() {
    const std::string basePrompt = "turing";
    const char* separator = ":";
    return basePrompt + separator + _turing.getDBName() + "> ";
}

TuringShell::Result TuringShell::processCommand(const std::string& line) {
    // Get first word
    const auto firstWord = getFirstWord(line);
    
    if (firstWord == "cd") {
        std::vector<std::string_view> words;
        extractWords(words, line);
        if (words.size() != 2) {
            logt::LogError("ERROR: the command 'cd' expects one argument");
            return BadResult(CommandError::ARG_EXPECTED);
        }

        _turing.setDBName(std::string(words[1]));
        return CommandType::LOCAL_CMD;
    } else if (firstWord == "exit" || firstWord == "quit" || firstWord == "q") {
        exit(EXIT_SUCCESS);
        return CommandType::LOCAL_CMD;
    }

    return CommandType::REMOTE_CMD;
}

void TuringShell::process(std::string& line) {
    // Remove leading whitespace
    trim(line);

    // Stop here if it was a special command
    const auto processRes = processCommand(line);
    if (processRes != CommandType::REMOTE_CMD) {
        return;
    }

    const auto timeExecStart = Clock::now();

    // Send query
    const bool success = _turing.query(line, _df);
    if (!success) {
        logt::LogError("ERROR: can not send request to server");
        return;
    }

    // Format result table
    displayTable();

    // Query execution time
    const auto timeExecEnd = Clock::now();
    const std::chrono::duration<double, std::milli> duration = timeExecEnd - timeExecStart;
    std::cout << "Request executed in " << duration.count() << " ms.\n";
}

void TuringShell::displayTable() {
    _table.reset();
}