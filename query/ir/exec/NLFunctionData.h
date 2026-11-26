#pragma once

#include <vector>

namespace db {

class NLExecutionContext;
class NLFunctionData;

// Type of function pointers implementing operations
using NLHandlerFunction = void (*)(NLExecutionContext* context, NLFunctionData* data);

// Function descriptor representing translated statements
// It consists of a function pointer and function-specific data
class NLFunctionDescriptor {
public:
    NLFunctionDescriptor(NLHandlerFunction function,
                         NLFunctionData* data)
        : _function(function),
        _data(data)
    {
    }

    NLHandlerFunction getFunction() const { return _function; }
    NLFunctionData* getData() const { return _data; }

private:
    NLHandlerFunction _function {nullptr};
    NLFunctionData* _data {nullptr};
};

// Base class of function-specific data
class NLFunctionData {
public:
    virtual ~NLFunctionData() = default;
};

// Holds the translated statements of a program or loop body
class NLStmtContainer {
public:
    using Stmts = std::vector<NLFunctionDescriptor>;

    const Stmts& stmts() const { return _stmts; }

    void addStmt(const NLFunctionDescriptor& stmt) {
        _stmts.push_back(stmt);
    }

    template <typename... Args>
    void emplaceStmt(Args&&... args) {
        _stmts.emplace_back(std::forward<Args>(args)...);
    }

private:
    Stmts _stmts;
};

}
