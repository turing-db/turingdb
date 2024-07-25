#pragma once

#include "ToolCommand.h"

#include <argparse.hpp>

class TuringStartCommand : public ToolCommand {
public:
    TuringStartCommand(ToolInit& toolInit);
    ~TuringStartCommand();

    void setup() override;
    bool isActive() override;
    void run() override;

private:
    argparse::ArgumentParser _startCommand;

    std::string getDBName();
    bool isPrototypeRequested();
    bool isDevRequested();
    bool isBuildRequested();
    bool noDemonRequested();
};
