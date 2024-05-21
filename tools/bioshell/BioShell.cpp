#include "TuringClient.h"

#include <iostream>
#include <chrono>
#include <ratio>
#include <string>
#include <algorithm>
#include <ctype.h>
#include <string_view>

#include <argparse.hpp>

#include "ToolInit.h"
#include "PerfStat.h"
#include "TimerStat.h"

#include "linenoise.h"

using namespace turing;

using Clock = std::chrono::system_clock;

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

// Returns true if the line has been processed as a special command
bool processCommand(TuringClient& turing, std::string& line) {
    // Get first word
    const auto firstWord = getFirstWord(line);
    
    if (firstWord == "cd") {
        std::vector<std::string_view> words;
        extractWords(words, line);
        if (words.size() != 2) {
            std::cout << "ERROR: the command 'cd' expects one argument.\n";
            return true;
        }

        turing.setDBName(std::string(words[1]));
        return true;
    } else if (firstWord == "exit" || firstWord == "quit" || firstWord == "q") {
        exit(EXIT_SUCCESS);
        return true;
    }

    return false;
}

void process(TuringClient& turing, std::string& line) {
    // Remove leading whitespace
    trim(line);

    // Stop here if it was a special command
    if (processCommand(turing, line)) {
        return;
    }

    const auto timeExecStart = Clock::now();

    const bool success = turing.exec(line);
    if (!success) {
        std::cout << "ERROR: can not send request to server.\n";
        return;
    }

    const auto timeExecEnd = Clock::now();
    const std::chrono::duration<double, std::milli> duration = timeExecEnd - timeExecStart;
    std::cout << "Request done in " << duration.count() << " ms.\n";
}

std::string composePrompt(const TuringClient& turing) {
    const std::string basePrompt = "turing";
    const char* separator = ":";
    return basePrompt + separator + turing.getDBName() + "> ";
}

}

int main(int argc, const char** argv) {
    ToolInit toolInit("bioshell");

    std::string dbName;
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-db")
             .help("Database name")
             .nargs(1)
             .store_into(dbName);        

    toolInit.init(argc, argv);

    TuringClient turing;
    turing.setDBName(dbName);

    std::string shellPrompt = composePrompt(turing);
    char* line = NULL;
    std::string lineStr;
    while ((line = linenoise(shellPrompt.c_str())) != NULL) {
        lineStr = line;
        if (lineStr.empty()) {
            continue;
        }

        process(turing, lineStr);

        linenoiseHistoryAdd(line);
        shellPrompt = composePrompt(turing);
    }

    return EXIT_SUCCESS;
}
