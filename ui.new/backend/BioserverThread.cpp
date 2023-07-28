#include "BioserverThread.h"
#include "Command.h"

namespace ui {

BioserverThread::BioserverThread()
{
}

BioserverThread::~BioserverThread() {
}

void BioserverThread::task() {
    Command cmd("bioserver");
    cmd.setGenerateScript(false);
    cmd.run();
}

}
