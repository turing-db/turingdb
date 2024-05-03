#include "TuringTool.h"

#include "TuringStartCommand.h"

TuringTool::TuringTool(ToolInit& toolInit)
    : _engine(toolInit)
{
    _engine.addCommand(std::make_unique<TuringStartCommand>(toolInit));
}

TuringTool::~TuringTool() {
}

void TuringTool::setup() {
    _engine.setup();
}

void TuringTool::run() {
    _engine.run();
}
