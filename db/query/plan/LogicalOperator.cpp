#include "LogicalOperator.h"

#include "Frame.h"

using namespace db::query;

LogicalOperator::~LogicalOperator() {
}

Cursor::~Cursor() {
}

// OutputTableOperator
OutputTableOperator::OutputTableOperator(const std::vector<Symbol*>& outputSymbols,
                                         Callback callback)
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
