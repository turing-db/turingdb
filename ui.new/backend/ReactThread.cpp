#include "ReactThread.h"
#include "Command.h"
#include "FileUtils.h"

namespace ui {

ReactThread::ReactThread()
{
}

ReactThread::~ReactThread() {
}

void ReactThread::devTask() {
#ifdef TURING_DEV
    FileUtils::Path currentPath = std::filesystem::current_path();
    FileUtils::Path turinguiPath {TURINGUI_BACKEND_SRC_DIR};
    _logFilePath = currentPath / "reports" / "react.log";
    turinguiPath = turinguiPath / "frontend";

    Command cmd("npm");
    cmd.addArg("--prefix");
    cmd.addArg(turinguiPath);
    cmd.addArg("start");
    cmd.setLogFile(_logFilePath);
    cmd.setGenerateScript(false);
    cmd.run();
    _returnCode = cmd.getReturnCode();
#endif
}

}
