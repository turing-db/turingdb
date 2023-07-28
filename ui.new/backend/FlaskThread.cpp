#include "FlaskThread.h"
#include "Command.h"
#include "FileUtils.h"

namespace ui {

FlaskThread::FlaskThread()
{
}

FlaskThread::~FlaskThread() {
}

void FlaskThread::task() {
    FileUtils::Path staticFolder = std::filesystem::current_path() / "site";

    Command cmd("python3");
    cmd.addArg("-m");
    cmd.addArg("turingui");
    cmd.addArg(staticFolder);
    cmd.setGenerateScript(false);
    cmd.run();
}

void FlaskThread::devTask() {
#ifdef TURING_DEV
    // Spawns the flask server with static folder "./" (not used in dev mode)
    FileUtils::Path turinguiPath {TURINGUI_BACKEND_SRC_DIR};
    turinguiPath = turinguiPath / "backend" / "turingui" / "__main__.py";

    Command cmd("python3");
    cmd.addArg(turinguiPath);
    cmd.addArg("./");
    cmd.addArg("--dev");
    cmd.setGenerateScript(false);
    cmd.run();
#endif
}

}
