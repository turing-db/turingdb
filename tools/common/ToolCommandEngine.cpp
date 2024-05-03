#include "ToolCommandEngine.h"

#include "ToolCommand.h"
#include "ToolInit.h"
#include "BannerDisplay.h"

ToolCommandEngine::ToolCommandEngine(ToolInit& toolInit)
    : _toolInit(toolInit)
{
}

ToolCommandEngine::~ToolCommandEngine() {
}

void ToolCommandEngine::addCommand(std::unique_ptr<ToolCommand> cmd) {
    _commands.emplace_back(std::move(cmd));
}

void ToolCommandEngine::setup() {
    for (auto& cmd : _commands) {
        cmd->setup();
    }
}

void ToolCommandEngine::run() {
    bool isActive = false;
    for (auto& cmd : _commands) {
        if (cmd->isActive()) {
            isActive = true;
            cmd->run();
        }
    }

    if (!isActive) {
        BannerDisplay::printBanner();
        _toolInit.printHelp();
    }
}
