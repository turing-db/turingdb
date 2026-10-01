#pragma once

#include "AbstractThreadContext.h"
#include "CompilerContext.h"
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
    CompilerContext& getCompilerContext() { return _compilerContext; }

private:
    LocalMemory _localMem;
    CompilerContext _compilerContext;
};

}
