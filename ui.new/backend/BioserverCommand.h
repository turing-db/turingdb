#pragma once

#include "ServerCommand.h"

namespace ui {

class BioserverCommand : public ServerCommand {
public:
    BioserverCommand();
    ~BioserverCommand() override;

    void run(ProcessGroup& group) override;
};

}
