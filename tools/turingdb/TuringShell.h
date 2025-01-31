#pragma once

#include <unordered_map>
#include <string>
#include <string_view>
#include <functional>
#include <vector>

class TuringShell {
public:
    struct Command {
        using Words = std::vector<std::string>;
        std::function<void(const Words&, TuringShell&)> _func;
    };

    TuringShell();
    ~TuringShell();

    void setGraphName(const std::string& graphName) { _graphName = graphName; }

    void startLoop();

    void printHelp() const;

private:
    std::string _graphName {"default"};
    std::unordered_map<std::string_view, Command> _localCommands;

    void processLine(std::string& line);
    std::string composePrompt();
};
