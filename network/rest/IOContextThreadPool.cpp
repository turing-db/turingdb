#include "IOContextThreadPool.h"

using namespace net;

IOContextThreadPool::IOContextThreadPool(size_t poolSize)
    : _poolSize(poolSize),
    _guard(_ioContext.get_executor())
{
}

IOContextThreadPool::~IOContextThreadPool() {
}

void IOContextThreadPool::run() {
    _threads.reserve(_poolSize);
    for (size_t i = 0; i < _poolSize; i++) {
        _threads.emplace_back([this]() { _ioContext.run(); });
    }
}

void IOContextThreadPool::shutdown() {
    _ioContext.stop();
}

void IOContextThreadPool::waitForShutdown() {
    for (auto& thread : _threads) {
        thread.join();
    }
}

