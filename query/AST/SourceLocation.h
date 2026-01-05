#pragma once

#include <cstdint>

namespace db::v2 {

struct SourceLocation {
    uint32_t beginLine {1};
    uint32_t beginColumn {1};
    uint32_t endLine {1};
    uint32_t endColumn {1};

    void step() {
        beginLine = endLine;
        beginColumn = endColumn;
    }

    void columns(uint32_t columns = 1) {
        endColumn += columns;
    }

    void lines(uint32_t lines = 1) {
        endLine += lines;
        endColumn = 1;
    }
};

#define YYLLOC_DEFAULT(Current, Rhs, N) \
    do { \
        if (N) { \
            (Current).beginLine = YYRHSLOC(Rhs, 1).beginLine; \
            (Current).beginColumn = YYRHSLOC(Rhs, 1).beginColumn; \
            (Current).endLine = YYRHSLOC(Rhs, N).endLine; \
            (Current).endColumn = YYRHSLOC(Rhs, N).endColumn; \
        } else { \
            (Current).beginLine = (Current).endLine = YYRHSLOC(Rhs, 0).endLine; \
            (Current).beginColumn = (Current).endColumn = YYRHSLOC(Rhs, 0).endColumn; \
        } \
    } while (0)

}
