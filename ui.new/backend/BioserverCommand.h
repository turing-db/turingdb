#pragma once

#include "ServerCommand.h"
#include "Command.h"

namespace ui {

class BioserverCommand : public ServerCommand {
public:
    BioserverCommand();
    ~BioserverCommand() override;

    void run(ProcessGroup& group) override;

private:
    Command _cmd;
};

}
