#pragma once

#include <memory>
#include <vector>

#include <argparse.hpp>

namespace db {

class StartCmd {
public:
    ~StartCmd();

    StartCmd(const StartCmd&) = delete;
    StartCmd(StartCmd&&) = delete;
    StartCmd& operator=(const StartCmd&) = delete;
    StartCmd& operator=(StartCmd&&) = delete;

    static std::unique_ptr<StartCmd> create();

    argparse::ArgumentParser& getArgParser() { return _argParser; }

    int execute();

private:
    argparse::ArgumentParser _argParser;
    std::vector<std::string> _graphsToLoad;
    std::string _turingDir;
    std::string _address {"127.0.0.1"};
    bool _demonize {false};
    bool _inMemory {false};
    bool _resetDefault {false};
    unsigned _port {6666};
    size_t _startTimeout {30};

    StartCmd();

    void initialize();
};

}
