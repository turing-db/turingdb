#pragma once

#include <memory>

#include <argparse.hpp>

namespace db {

class StopCmd {
public:
    ~StopCmd();

    StopCmd(const StopCmd&) = delete;
    StopCmd(StopCmd&&) = delete;
    StopCmd& operator=(const StopCmd&) = delete;
    StopCmd& operator=(StopCmd&&) = delete;

    static std::unique_ptr<StopCmd> create();

    argparse::ArgumentParser& getArgParser() { return _argParser; }

    int execute();

private:
    argparse::ArgumentParser _argParser;
    std::string _turingDir;

    StopCmd();

    void initialize();
};

}
