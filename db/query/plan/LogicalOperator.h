#pragma once

#include <vector>
#include <functional>

#include "Value.h"

namespace db::query {

class Frame;
class ExecutionContext;
class Symbol;

class Cursor {
public:
    virtual ~Cursor();
    virtual bool pull(Frame& frame, ExecutionContext* ctxt) = 0;
};

class LogicalOperator {
public:
    virtual ~LogicalOperator();

    const std::vector<Symbol*>& getOutputSymbols() const { return _outputSymbols; }

    virtual Cursor* makeCursor() = 0;

protected:
    std::vector<Symbol*> _outputSymbols;
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

        bool pull(Frame& frame, ExecutionContext* ctxt) override;

    private:
        OutputTableOperator* _self {nullptr};
        Rows _rows;
        bool _pulled {false};
        size_t _currentRow {0};
    };
};

}
