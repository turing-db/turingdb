#pragma once

#include <string>
#include <fstream>
#include <ostream>

#include "FileUtils.h"

class Report {
public:
    Report(const FileUtils::Path& reportFilePath,
           const std::string& title);
    ~Report();

    std::ostream& getStream() { return _stream; }

private:
    std::ofstream _stream;

    void printHeaderLine();
};
