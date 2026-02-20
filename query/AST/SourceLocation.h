#pragma once

#include <stdint.h>

namespace db {

struct SourceLocation {
    uint32_t _beginLine {1};
    uint32_t _beginColumn {1};
    uint32_t _endLine {1};
    uint32_t _endColumn {1};

    void step() {
        _beginLine = _endLine;
        _beginColumn = _endColumn;
    }

    void columns(uint32_t columns = 1) {
        _endColumn += columns;
    }

    void lines(uint32_t lines = 1) {
        _endLine += lines;
        _endColumn = 1;
    }
};

#define YYLLOC_DEFAULT(Current, Rhs, N) \
    do { \
        if (N) { \
            (Current)._beginLine = YYRHSLOC(Rhs, 1)._beginLine; \
            (Current)._beginColumn = YYRHSLOC(Rhs, 1)._beginColumn; \
            (Current)._endLine = YYRHSLOC(Rhs, N)._endLine; \
            (Current)._endColumn = YYRHSLOC(Rhs, N)._endColumn; \
        } else { \
            (Current)._beginLine = (Current)._endLine = YYRHSLOC(Rhs, 0)._endLine; \
            (Current)._beginColumn = (Current)._endColumn = YYRHSLOC(Rhs, 0)._endColumn; \
        } \
    } while (0)

}
