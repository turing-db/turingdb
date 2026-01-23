#pragma once

#include <stddef.h>

namespace net {

class HTTPServer;

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

    friend HTTPServer;
    void setThreadID(size_t threadID) { _threadID = threadID; }
};

}
