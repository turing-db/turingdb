#pragma once

#include <memory>
#include <vector>

class ToolInit;
class TuringToolComponent;

class TuringTool {
public:
    TuringTool(ToolInit& toolInit);
    ~TuringTool();

    void setup();
    void run();

private:
    ToolInit& _toolInit;
    std::vector<std::unique_ptr<TuringToolComponent>> _components;
};
