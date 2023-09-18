#pragma once

#include "ServerCommand.h"
#include "Command.h"

namespace ui {

class ReactCommand : public ServerCommand {
public:
    ReactCommand();
    ~ReactCommand() override;

    void runDev(ProcessGroup& group) override;

private:
    Command _cmd;
};

}
