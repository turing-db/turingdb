#pragma once

#include "ToolCommand.h"

#include <argparse.hpp>

class TuringStopCommand : public ToolCommand {
public:
    TuringStopCommand(ToolInit& toolInit);
    ~TuringStopCommand();

    void setup() override;
    bool isActive() override;
    void run() override;

private:
    argparse::ArgumentParser _stopCommand;
};
