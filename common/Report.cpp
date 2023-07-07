#include "Report.h"

Report::Report(const FileUtils::Path& reportFilePath,
               const std::string& title)
    : _stream(reportFilePath)
{
    if (_stream.is_open()) {
        printHeaderLine();
        _stream << "        " << title << '\n';
        printHeaderLine();
        _stream << '\n';
    }
}

Report::~Report() {
}

void Report::printHeaderLine() {
    for (int i = 0; i < 40; i++) {
        _stream << '=';
    }
    _stream << '\n';
}
