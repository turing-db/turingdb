#pragma once

#include "ToolCommand.h"

#include <argparse.hpp>

class TuringStart2Command : public ToolCommand {
public:
    explicit TuringStart2Command(ToolInit& toolInit);
    ~TuringStart2Command() override;

    TuringStart2Command(const TuringStart2Command&) = delete;
    TuringStart2Command(TuringStart2Command&&) = delete;
    TuringStart2Command& operator=(const TuringStart2Command&) = delete;
    TuringStart2Command& operator=(TuringStart2Command&&) = delete;

    void setup() override;
    bool isActive() override;
    void run() override;

private:
    argparse::ArgumentParser _startCommand;
    uint32_t _turingDBPort = 6666;

    bool isDevRequested();
    bool isBuildRequested();
    bool noDemonRequested();
};
