#include "TuringTool.h"

#include "TuringServerComponent.h"
#include "ToolInit.h"
#include "BannerDisplay.h"

TuringTool::TuringTool(ToolInit& toolInit)
    : _toolInit(toolInit)
{
    _components.emplace_back(new TuringServerComponent(toolInit));
}

TuringTool::~TuringTool() {
}

void TuringTool::setup() {
    for (auto& component : _components) {
        component->setup();
    }
}

void TuringTool::run() {
    bool inactive = true;
    for (auto& component : _components) {
        if (component->isActive()) {
            inactive = false;
            component->run();
        }
    }

    if (inactive) {
        BannerDisplay::printBanner();
        _toolInit.printHelp();
    }
}
