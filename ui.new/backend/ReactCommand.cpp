#include "ReactCommand.h"
#include "FileUtils.h"

namespace ui {

ReactCommand::ReactCommand()
    : ServerCommand("React"),
      _cmd("npm")
{
}

ReactCommand::~ReactCommand() {
}

void ReactCommand::runDev(ProcessGroup& group) {
#ifdef TURING_DEV
    FileUtils::Path currentPath = std::filesystem::current_path();
    FileUtils::Path turinguiPath {TURINGUI_BACKEND_SRC_DIR};
    _logFilePath = currentPath / "reports" / "react.log";
    turinguiPath = turinguiPath / "frontend";

    _cmd.addArg("--prefix");
    _cmd.addArg(turinguiPath);
    _cmd.addArg("run");
    _cmd.addArg("dev");
    _cmd.setLogFile(_logFilePath);
    _cmd.setScriptPath("npm_run_dev.sh");
    _cmd.setGenerateScript(true);
    _process = _cmd.runAsync(group);
#endif
}

}
