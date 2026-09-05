#pragma once

#include <stddef.h>
#include <iosfwd>
#include <string>
#include <string_view>
#include <vector>

namespace db {

// A result table sized to the terminal: the columns share the width the terminal has,
// and a cell line longer than its column is wrapped under a hanging indent rather than
// pushing the box past the right edge, where the terminal would rewrap it over the borders.
class ShellTable {
public:
    ShellTable();
    ~ShellTable();

    void startRow();
    void addCell(std::string_view value);

    void print(std::ostream& out) const;

private:
    std::vector<std::vector<std::string>> _rows;
    size_t _columnCount {0};

    void computeColumnWidths(std::vector<size_t>& widths) const;
};

}
