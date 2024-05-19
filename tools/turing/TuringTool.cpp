#include "TuringTool.h"

#include "TuringStartCommand.h"
#include "TuringStart2Command.h"
#include "TuringStopCommand.h"

TuringTool::TuringTool(ToolInit& toolInit)
    : _engine(toolInit)
{
    _engine.addCommand(std::make_unique<TuringStartCommand>(toolInit));
    _engine.addCommand(std::make_unique<TuringStart2Command>(toolInit));
    _engine.addCommand(std::make_unique<TuringStopCommand>(toolInit));
}

TuringTool::~TuringTool() {
}

void TuringTool::setup() {
    _engine.setup();
}

void TuringTool::run() {
    _engine.run();
}
