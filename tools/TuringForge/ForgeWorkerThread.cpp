#include "ForgeWorkerThread.h"

using namespace forge;

ForgeWorkerThread::ForgeWorkerThread(const ForgeConfig* config)
    : _config(config)
{
}

ForgeWorkerThread::~ForgeWorkerThread() {
}

void ForgeWorkerThread::init() {
    _wallClockHistogram.init();
    _engineHistogram.init();
    _changeCycleHistogram.init();
}
