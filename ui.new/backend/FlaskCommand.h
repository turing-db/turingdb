#pragma once

#include "ServerCommand.h"
#include "Command.h"

namespace ui {

class FlaskCommand : public ServerCommand {
public:
    FlaskCommand();
    ~FlaskCommand() override;

    void run(ProcessGroup& group) override;
    void runDev(ProcessGroup& group) override;

private:
    Command _cmd;
};

}
