#pragma once

#include "NLProgram.h"

namespace db {

// The cursor an unwind loop and a list comprehension both walk: every (row, element) pair
// of a source column in row order. A cell contributing nothing - a null, an empty list -
// is skipped, so it emits no row. The cursor is local to the walk, so one nested in an
// outer loop restarts on every one of its steps.
class NLElementCursor {
public:
    NLElementCursor(const Column* source, NLUnwindElementCountFunction elementCount)
        : _source(source),
        _elementCount(elementCount),
        _sourceRows(source->size())
    {
        openNextRow();
    }

    bool exhausted() const { return _sourceRow >= _sourceRows; }
    size_t getRow() const { return _sourceRow; }
    size_t getSourceRows() const { return _sourceRows; }

    // Takes the next @param chunkSize pairs, or what is left of them: the source row each
    // pair came from, and - for a source whose cells hold more than the element - which
    // element of its cell the pair took, which the emit handler reads.
    void fillChunk(size_t chunkSize, std::vector<size_t>& rows, std::vector<size_t>* positions) {
        rows.clear();
        if (positions) {
            positions->clear();
        }

        while (rows.size() < chunkSize && !exhausted()) {
            rows.push_back(_sourceRow);

            if (positions) {
                positions->push_back(_elementIndex);
            }

            _elementIndex++;

            if (_elementIndex == _rowElements) {
                skipRow();
            }
        }
    }

    void skipRow() {
        _elementIndex = 0;
        _sourceRow++;
        openNextRow();
    }

private:
    const Column* _source {nullptr};
    NLUnwindElementCountFunction _elementCount {nullptr};
    size_t _sourceRows {0};
    size_t _sourceRow {0};
    size_t _elementIndex {0};
    size_t _rowElements {0};

    void openNextRow() {
        while (_sourceRow < _sourceRows) {
            _rowElements = _elementCount(_source, _sourceRow);
            if (_rowElements > 0) {
                return;
            }

            _sourceRow++;
        }
    }
};

}
