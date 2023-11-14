#include "FlaskCommand.h"
#include "Command.h"
#include "FileUtils.h"

namespace ui {

FlaskCommand::FlaskCommand()
    : ServerCommand("Flask"),
      _cmd("python3")
{
}

FlaskCommand::~FlaskCommand() = default;

void FlaskCommand::run(ProcessGroup& group) {
    const FileUtils::Path currentPath = std::filesystem::current_path();
    const FileUtils::Path staticFolder = currentPath / "site";
    _logFilePath = currentPath / "reports" / "turingui.log";

    _cmd.addArg("-m");
    _cmd.addArg("turingui");
    _cmd.addArg(staticFolder);
    _cmd.setLogFile(_logFilePath);
    _cmd.setScriptPath("start_flask.sh");
    _cmd.setGenerateScript(true);
    _process = _cmd.runAsync(group);
}

void FlaskCommand::runDev(ProcessGroup& group) {
#ifdef TURING_DEV
    // Spawns the flask server with static folder "./" (not used in dev mode)
    const FileUtils::Path currentPath = std::filesystem::current_path();
    const FileUtils::Path turinguiPath = FileUtils::Path(TURINGUI_BACKEND_SRC_DIR)
            / "backend" / "turingui" / "__main__.py";
    _logFilePath = currentPath / "reports" / "turingui.log";

    _cmd.addArg(turinguiPath);
    _cmd.addArg("./");
    _cmd.addArg("--dev");
    _cmd.setLogFile(_logFilePath);
    _cmd.setScriptPath("start_flask.sh");
    _cmd.setGenerateScript(true);
    _process = _cmd.runAsync(group);
#endif
}

}
