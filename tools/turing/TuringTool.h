#pragma once

#include "ToolCommandEngine.h"

class TuringTool {
public:
    TuringTool(ToolInit& toolInit);
    ~TuringTool();

    void setup();
    void run();

private:
    ToolCommandEngine _engine;
};
