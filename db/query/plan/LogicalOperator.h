#pragma once

#include <vector>
#include <functional>
#include <string>

#include "Value.h"
#include "PullStatus.h"

namespace db {

class Frame;
class ExecutionContext;
class Symbol;

class Cursor {
public:
    virtual ~Cursor();
    virtual PullStatus pull(Frame& frame, ExecutionContext* ctxt) = 0;
};

class LogicalOperator {
public:
    LogicalOperator(const std::vector<Symbol*>& outputSymbols);
    virtual ~LogicalOperator();

    const std::vector<Symbol*>& getOutputSymbols() const { return _outputSymbols; }

    virtual Cursor* makeCursor() = 0;

protected:
    std::vector<Symbol*> _outputSymbols;
};

class OpenDBOperator : public LogicalOperator {
public:
    OpenDBOperator(const std::string& name,
                   const std::vector<Symbol*>& outputSymbols);
    ~OpenDBOperator();

    Cursor* makeCursor() override;

private:
    const std::string _name;

    class OpenDBCursor : public Cursor {
    public:
        OpenDBCursor(OpenDBOperator* self);
        ~OpenDBCursor();

        PullStatus pull(Frame& frame, ExecutionContext* ctxt) override;

    private:
        OpenDBOperator* _self {nullptr};
    };
};

class OutputTableOperator : public LogicalOperator {
public:
    using Rows = std::vector<std::vector<db::Value>>;
    using Callback = std::function<void(Frame&,
                                        ExecutionContext*,
                                        Rows&)>;

    OutputTableOperator(const std::vector<Symbol*>& outputSymbols,
                        Callback callback);
    ~OutputTableOperator();

    Cursor* makeCursor() override;

private:
    Callback _callback;

    class OutputTableCursor : public Cursor {
    public:
        OutputTableCursor(OutputTableOperator* self);
        ~OutputTableCursor();

        PullStatus pull(Frame& frame, ExecutionContext* ctxt) override;

    private:
        OutputTableOperator* _self {nullptr};
        Rows _rows;
        bool _pulled {false};
        size_t _currentRow {0};
    };
};

}
