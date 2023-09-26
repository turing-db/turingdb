#include "LogicalOperator.h"

#include "Frame.h"
#include "ExecutionContext.h"
#include "InterpreterContext.h"
#include "DBManager.h"

using namespace db;

LogicalOperator::LogicalOperator(const std::vector<Symbol*>& outputSymbols)
    : _outputSymbols(outputSymbols)
{
}

LogicalOperator::~LogicalOperator() {
}

Cursor::~Cursor() {
}

// OpenDBOperator
OpenDBOperator::OpenDBOperator(const std::string& name,
                               const std::vector<Symbol*>& outputSymbols)
    : LogicalOperator(outputSymbols),
    _name(name)
{
}

OpenDBOperator::~OpenDBOperator() {
}

Cursor* OpenDBOperator::makeCursor() {
    OpenDBCursor* cursor = new OpenDBCursor(this);
    return cursor;
}

OpenDBOperator::OpenDBCursor::OpenDBCursor(OpenDBOperator* self)
    : _self(self)
{
}

OpenDBOperator::OpenDBCursor::~OpenDBCursor() {
}

bool OpenDBOperator::OpenDBCursor::pull(Frame& frame, ExecutionContext* ctxt) {
    DBManager* dbMan = ctxt->getInterpreterContext()->getDBManager();
    return dbMan->loadDB(_self->_name);
}

// OutputTableOperator
OutputTableOperator::OutputTableOperator(const std::vector<Symbol*>& outputSymbols,
                                         Callback callback)
    : LogicalOperator(outputSymbols),
    _callback(callback)
{
}

OutputTableOperator::~OutputTableOperator() {
}

OutputTableOperator::OutputTableCursor::OutputTableCursor(OutputTableOperator* self)
    : _self(self)
{
}

Cursor* OutputTableOperator::makeCursor() {
    OutputTableCursor* cursor = new OutputTableCursor(this);
    return cursor;
}

OutputTableOperator::OutputTableCursor::~OutputTableCursor() {
}

bool OutputTableOperator::OutputTableCursor::pull(Frame& frame, ExecutionContext* ctxt) {
    if (!_pulled) {
        _self->_callback(frame, ctxt, _rows);
        _pulled = true;
    }

    if (_currentRow < _rows.size()) {
        const auto& outputSymbols = _self->_outputSymbols;
        auto& row = _rows[_currentRow];
        for (size_t i = 0; i < row.size(); i++) {
            frame[outputSymbols[i]] = row[i];
        }
        _currentRow++;
        return true;
    }

    return false;
}
