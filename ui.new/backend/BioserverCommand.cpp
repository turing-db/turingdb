#include "BioserverCommand.h"

namespace ui {

BioserverCommand::BioserverCommand()
    : ServerCommand("Bioserver"),
    _cmd("bioserver")
{
}

BioserverCommand::~BioserverCommand() = default;

void BioserverCommand::run(ProcessGroup& group) {
    const FileUtils::Path currentPath = std::filesystem::current_path();
    _logFilePath = currentPath / "reports" / "bioserver_cmd.log";

    _cmd.setLogFile(_logFilePath);
    _cmd.setScriptPath("bioserver.sh");
    _cmd.setGenerateScript(true);
    _process = _cmd.runAsync(group);
}

}
