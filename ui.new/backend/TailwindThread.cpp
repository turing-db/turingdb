#include "TailwindThread.h"
#include "Command.h"
#include "FileUtils.h"

namespace ui {

TailwindThread::TailwindThread()
{
}

TailwindThread::~TailwindThread() {
}

void TailwindThread::devTask() {
#ifdef TURING_DEV
    FileUtils::Path turinguiPath {TURINGUI_BACKEND_SRC_DIR};
    FileUtils::Path currentPath = std::filesystem::current_path();
    turinguiPath = turinguiPath / "frontend";
    Command cmd("npm");
    cmd.addArg("exec");
    cmd.addArg("--prefix");
    cmd.addArg(turinguiPath.string());
    cmd.addArg("tailwindcss");
    cmd.addArg("-i");
    cmd.addArg("./src/input.css");
    cmd.addArg("-o");
    cmd.addArg("./src/style.css");
    cmd.addArg("--watch");
    cmd.setLogFile(currentPath / "tailwindcss.log");
    cmd.setGenerateScript(false);
    cmd.run();
#endif
}

}
