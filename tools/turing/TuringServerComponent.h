#pragma once

#include <argparse.hpp>

#include "TuringToolComponent.h"

class TuringServerComponent : public TuringToolComponent {
public:
    TuringServerComponent(ToolInit& toolInit);
    ~TuringServerComponent();

    void setup() override;

    bool isActive() const override;

    void run() override;

private:
    bool _isPrototypeMode {false};
    argparse::ArgumentParser _serverCommand;
    argparse::ArgumentParser _serverStartCommand;
    argparse::ArgumentParser _serverStopCommand;
};
