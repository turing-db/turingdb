#include "ShellTable.h"

#include <stddef.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <algorithm>
#include <numeric>
#include <ostream>

#include "BioAssert.h"

using namespace db;

namespace {

constexpr size_t fallbackTerminalWidth = 80;
constexpr size_t minimumColumnWidth = 4;
constexpr size_t hangingIndentWidth = 2;

// "| " on the left of a cell and " " on its right
constexpr size_t cellDecorationWidth = 3;

size_t getTerminalWidth() {
    winsize window {};
    const bool hasWindowSize = ioctl(STDOUT_FILENO, TIOCGWINSZ, &window) == 0;

    if (hasWindowSize && window.ws_col > 0) {
        return window.ws_col;
    }

    return fallbackTerminalWidth;
}

std::string_view trimTrailingNewLines(std::string_view text) {
    while (!text.empty() && text.back() == '\n') {
        text.remove_suffix(1);
    }

    return text;
}

bool isContinuationByte(char byte) {
    return (static_cast<unsigned char>(byte) & 0xC0) == 0x80;
}

size_t getDisplayWidth(std::string_view text) {
    const ptrdiff_t characters = std::count_if(text.begin(), text.end(), [](char byte) { return !isContinuationByte(byte); });

    return static_cast<size_t>(characters);
}

// Where the given number of characters ends, so a cut never splits a multi-byte character
size_t getByteOffset(std::string_view line, size_t characters) {
    size_t offset = 0;

    for (size_t character = 0; character < characters && offset < line.size(); character++) {
        offset++;

        while (offset < line.size() && isContinuationByte(line[offset])) {
            offset++;
        }
    }

    return offset;
}

size_t getTextWidth(std::string_view text) {
    size_t maxWidth = 0;
    size_t start = 0;

    while (start <= text.size()) {
        const size_t newLine = text.find('\n', start);
        const size_t end = (newLine == std::string_view::npos) ? text.size() : newLine;

        maxWidth = std::max(maxWidth, getDisplayWidth(text.substr(start, end - start)));

        if (newLine == std::string_view::npos) {
            return maxWidth;
        }

        start = newLine + 1;
    }

    return maxWidth;
}

size_t getIndentWidth(std::string_view line) {
    const size_t firstWord = line.find_first_not_of(' ');

    return (firstWord == std::string_view::npos) ? 0 : firstWord;
}

size_t findBreak(std::string_view line, size_t width) {
    const size_t end = getByteOffset(line, width);

    if (end == line.size()) {
        return end;
    }

    const size_t space = line.find_last_of(' ', end);
    const bool breaksOnSpace = space != std::string_view::npos && space > getIndentWidth(line);

    if (breaksOnSpace) {
        return space;
    }

    return end;
}

void wrapLine(std::string_view line, size_t width, std::vector<std::string>& lines) {
    if (getDisplayWidth(line) <= width) {
        lines.emplace_back(line);
        return;
    }

    const size_t indent = std::min(getIndentWidth(line) + hangingIndentWidth, width / 2);
    const std::string hangingIndent(indent, ' ');

    bool firstLine = true;
    while (!line.empty()) {
        const size_t available = firstLine ? width : width - indent;
        const size_t end = findBreak(line, available);

        std::string& wrapped = lines.emplace_back(firstLine ? std::string() : hangingIndent);
        wrapped += line.substr(0, end);

        line.remove_prefix(end);

        const size_t nextWord = line.find_first_not_of(' ');
        line.remove_prefix((nextWord == std::string_view::npos) ? line.size() : nextWord);

        firstLine = false;
    }
}

void wrapCell(std::string_view text, size_t width, std::vector<std::string>& lines) {
    text = trimTrailingNewLines(text);

    size_t start = 0;
    while (start <= text.size()) {
        const size_t newLine = text.find('\n', start);
        const size_t end = (newLine == std::string_view::npos) ? text.size() : newLine;

        wrapLine(text.substr(start, end - start), width, lines);

        if (newLine == std::string_view::npos) {
            return;
        }

        start = newLine + 1;
    }
}

// Narrows the widest columns first, so a single long column does not steal the
// budget from the short ones next to it
void shrinkToBudget(std::vector<size_t>& widths, size_t budget) {
    std::vector<size_t> narrowestFirst(widths.size());
    std::iota(narrowestFirst.begin(), narrowestFirst.end(), 0);
    std::sort(narrowestFirst.begin(),
              narrowestFirst.end(),
              [&widths](size_t left, size_t right) { return widths[left] < widths[right]; });

    size_t remaining = budget;
    size_t columnsLeft = widths.size();

    for (const size_t column : narrowestFirst) {
        const size_t share = remaining / columnsLeft;

        if (widths[column] > share) {
            widths[column] = std::max(share, minimumColumnWidth);
        }

        remaining -= std::min(remaining, widths[column]);
        columnsLeft--;
    }
}

void printSeparator(std::ostream& out, const std::vector<size_t>& widths) {
    for (const size_t width : widths) {
        out << '+' << std::string(width + cellDecorationWidth - 1, '-');
    }

    out << "+\n";
}

void printRow(std::ostream& out, const std::vector<std::string>& row, const std::vector<size_t>& widths) {
    std::vector<std::vector<std::string>> cells(widths.size());
    size_t height = 1;

    for (size_t column = 0; column < widths.size(); column++) {
        if (column < row.size()) {
            wrapCell(row[column], widths[column], cells[column]);
        }

        height = std::max(height, cells[column].size());
    }

    for (size_t line = 0; line < height; line++) {
        for (size_t column = 0; column < widths.size(); column++) {
            const std::vector<std::string>& cell = cells[column];
            const std::string_view text = (line < cell.size()) ? std::string_view(cell[line]) : std::string_view();
            const size_t textWidth = getDisplayWidth(text);
            const size_t padding = (textWidth < widths[column]) ? widths[column] - textWidth : 0;

            out << "| " << text << std::string(padding + 1, ' ');
        }

        out << "|\n";
    }
}

}

ShellTable::ShellTable() {
}

ShellTable::~ShellTable() {
}

void ShellTable::startRow() {
    _rows.emplace_back();
}

void ShellTable::addCell(std::string_view value) {
    bioassert(!_rows.empty(), "A shell table cell was added outside of a row");

    std::vector<std::string>& row = _rows.back();
    row.emplace_back(value);

    _columnCount = std::max(_columnCount, row.size());
}

void ShellTable::computeColumnWidths(std::vector<size_t>& widths) const {
    widths.assign(_columnCount, 0);

    for (const std::vector<std::string>& row : _rows) {
        for (size_t column = 0; column < row.size(); column++) {
            widths[column] = std::max(widths[column], getTextWidth(row[column]));
        }
    }

    const size_t naturalWidth = std::accumulate(widths.begin(), widths.end(), size_t {0});
    const size_t decorationWidth = _columnCount * cellDecorationWidth + 1;
    const size_t terminalWidth = getTerminalWidth();

    if (naturalWidth + decorationWidth <= terminalWidth) {
        return;
    }

    const size_t smallestTable = decorationWidth + _columnCount * minimumColumnWidth;
    const size_t budget = (terminalWidth > smallestTable) ? terminalWidth - decorationWidth : _columnCount * minimumColumnWidth;

    shrinkToBudget(widths, budget);
}

void ShellTable::print(std::ostream& out) const {
    if (_rows.empty()) {
        return;
    }

    std::vector<size_t> widths;
    computeColumnWidths(widths);

    printSeparator(out, widths);

    for (const std::vector<std::string>& row : _rows) {
        printRow(out, row, widths);
        printSeparator(out, widths);
    }
}
