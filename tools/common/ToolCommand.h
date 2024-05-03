#pragma once

class ToolInit;

class ToolCommand {
public:
    ToolCommand(ToolInit& toolInit);
    virtual ~ToolCommand() = default;

    virtual void setup() = 0;

    virtual bool isActive() = 0;

    virtual void run() = 0;

protected:
    ToolInit& _toolInit;
};
