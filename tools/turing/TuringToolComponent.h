#pragma once

class ToolInit;

class TuringToolComponent {
public:
    TuringToolComponent(ToolInit& toolInit);
    virtual ~TuringToolComponent();

    virtual void setup() = 0;

    virtual bool isActive() const = 0;

    virtual void run() = 0;

protected:
    ToolInit& _toolInit;
};
