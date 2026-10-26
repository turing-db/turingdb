#pragma once

#include <stddef.h>
#include <stdint.h>

namespace db {

struct SourceLocation {
    uint32_t _beginLine {1};
    uint32_t _beginColumn {1};
    uint32_t _endLine {1};
    uint32_t _endColumn {1};

    // Byte offsets in the query: what the location spans is the query text between them
    size_t _beginOffset {0};
    size_t _endOffset {0};

    void step() {
        _beginLine = _endLine;
        _beginColumn = _endColumn;
        _beginOffset = _endOffset;
    }

    void columns(uint32_t columns = 1) {
        _endColumn += columns;
    }

    void lines(uint32_t lines = 1) {
        _endLine += lines;
        _endColumn = 1;
    }

    // From the start of first to the end of last
    static SourceLocation join(const SourceLocation& first, const SourceLocation& last) {
        return SourceLocation {
            ._beginLine = first._beginLine,
            ._beginColumn = first._beginColumn,
            ._endLine = last._endLine,
            ._endColumn = last._endColumn,
            ._beginOffset = first._beginOffset,
            ._endOffset = last._endOffset,
        };
    }
};

#define YYLLOC_DEFAULT(Current, Rhs, N) \
    do { \
        if (N) { \
            (Current) = SourceLocation::join(YYRHSLOC(Rhs, 1), YYRHSLOC(Rhs, N)); \
        } else { \
            (Current) = YYRHSLOC(Rhs, 0); \
            (Current).step(); \
        } \
    } while (0)

}
