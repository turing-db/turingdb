#pragma once

#include <string>
#include <memory>

#include "FileUtils.h"

namespace argparse {
class ArgumentParser;
}

class ToolInit {
public:
    ToolInit(const std::string& toolName);
    ~ToolInit();

    void setOutputDir(const std::string& outDir);

    void disableOutputDir() { _outputDirEnabled = false; }
    void createOutputDir();

    void printHelp() const;

    void init(int argc, const char** argv);

    const std::string& getOutputsDir() const { return _outputsDir; }
    FileUtils::Path getOutputsDirPath() const { return FileUtils::Path(_outputsDir); }

    const std::string& getReportsDir() const { return _reportsDir; }
    FileUtils::Path getReportsDirPath() const { return FileUtils::Path(_reportsDir); }

    argparse::ArgumentParser& getArgParser() { return *_argParser; }

private:
    const std::string _toolName;
    std::string _outputsDir;
    std::string _reportsDir;
    std::unique_ptr<argparse::ArgumentParser> _argParser;
    bool _outputDirEnabled {true};
    std::string _logFilePath;

    void setupArgParser();
    void parseArguments(int argc, const char** argv);
    void setupLogger();
};
