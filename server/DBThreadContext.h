#pragma once

#include "AbstractThreadContext.h"
#include "IRContext.h"
#include "LocalMemory.h"

namespace db {

class DBThreadContext : public net::AbstractThreadContext {
public:
    DBThreadContext() = default;

    ~DBThreadContext() override = default;

    DBThreadContext(const DBThreadContext&) = delete;
    DBThreadContext(DBThreadContext&&) = delete;
    DBThreadContext& operator=(const DBThreadContext&) = delete;
    DBThreadContext& operator=(DBThreadContext&&) = delete;

    LocalMemory& getLocalMemory() { return _localMem; }
    IRContext& getIRContext() { return _irContext; }

private:
    LocalMemory _localMem;
    IRContext _irContext;
};

}
