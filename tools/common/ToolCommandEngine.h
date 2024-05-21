#pragma once

#include <vector>
#include <memory>

class ToolInit;
class ToolCommand;

class ToolCommandEngine {
public:
    ToolCommandEngine(ToolInit& toolInit);
    ~ToolCommandEngine();

    void addCommand(std::unique_ptr<ToolCommand> cmd);

    void setup();
    void run();

private:
    ToolInit& _toolInit;
    std::vector<std::unique_ptr<ToolCommand>> _commands;
};
