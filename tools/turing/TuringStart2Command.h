#pragma once

#include "ToolCommand.h"

#include <argparse.hpp>

class TuringStart2Command : public ToolCommand {
public:
    TuringStart2Command(ToolInit& toolInit);
    ~TuringStart2Command();

    void setup() override;
    bool isActive() override;
    void run() override;

private:
    argparse::ArgumentParser _startCommand;
};
