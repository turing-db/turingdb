#pragma once

#include <cstddef>

namespace net {

class Server;

class AbstractThreadContext {
public:
    AbstractThreadContext() = default;
    virtual ~AbstractThreadContext() = default;

    AbstractThreadContext(const AbstractThreadContext&) = default;
    AbstractThreadContext(AbstractThreadContext&&) = default;
    AbstractThreadContext& operator=(const AbstractThreadContext&) = default;
    AbstractThreadContext& operator=(AbstractThreadContext&&) = default;

    size_t getThreadID() const { return _threadID; }

private:
    size_t _threadID {};

    friend Server;
    void setThreadID(size_t threadID) { _threadID = threadID; }
};

}
