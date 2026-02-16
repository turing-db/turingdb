#pragma once

#include <memory>
#include <vector>

namespace argparse {
class ArgumentParser;
}

namespace db {

class StartCmd {
public:
    StartCmd();
    ~StartCmd();

    StartCmd(const StartCmd&) = delete;
    StartCmd(StartCmd&&) = delete;
    StartCmd& operator=(const StartCmd&) = delete;
    StartCmd& operator=(StartCmd&&) = delete;

    argparse::ArgumentParser& getArgParser() { return *_argParser; }

    int execute();

private:
    std::unique_ptr<argparse::ArgumentParser> _argParser;

    std::vector<std::string> _graphsToLoad;
    std::string _turingDir;
    std::string _address = "127.0.0.1";
    bool _demonize {false};
    bool _inMemory {false};
    bool _resetDefault {false};
    unsigned _port {6666};
};

}
