#include "BioserverThread.h"
#include "Command.h"

namespace ui {

BioserverThread::BioserverThread()
{
}

BioserverThread::~BioserverThread() {
}

void BioserverThread::task() {
    FileUtils::Path currentPath = std::filesystem::current_path();
    _logFilePath = currentPath / "bioserver.out" / "reports" / "bioserver.log";
    Command cmd("bioserver");
    cmd.setGenerateScript(false);
    cmd.setLogFile(currentPath / "reports" / "bioserverThread.log");
    cmd.run();
    _returnCode = cmd.getReturnCode();
}

}
