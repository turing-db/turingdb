#pragma once

#include <memory>

namespace argparse {
class ArgumentParser;
}

namespace db {

class StopCmd {
public:
    StopCmd();
    ~StopCmd();

    StopCmd(const StopCmd&) = delete;
    StopCmd(StopCmd&&) = delete;
    StopCmd& operator=(const StopCmd&) = delete;
    StopCmd& operator=(StopCmd&&) = delete;

    argparse::ArgumentParser& getArgParser() { return *_argParser; }

    int execute();

private:
    std::unique_ptr<argparse::ArgumentParser> _argParser;
    std::string _turingDir;
};

}
