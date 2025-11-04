#include "ToolInit.h"

#include <iostream>
#include <stdlib.h>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "FileUtils.h"
#include "PerfStat.h"
#include "LogSetup.h"

namespace {

void atexitHandler() {
    LogSetup::logFlush();
    std::cout << "\n";
}

}

ToolInit::ToolInit(const std::string& toolName)
    : _toolName(toolName),
    _argParser(new argparse::ArgumentParser(toolName))
{
}

ToolInit::~ToolInit() {
    PerfStat::destroy();
}

void ToolInit::setOutputDir(const std::string& outDir) {
    _outputsDir = outDir;
}

void ToolInit::setupArgParser() {
    auto& outArg = _argParser->add_argument("-o");
    outArg.help("Change the default output directory");
    outArg.metavar("out_dir");
    outArg.nargs(1);
    outArg.store_into(_outputsDir);
}

void ToolInit::parseArguments(int argc, const char** argv) {
    try {
        _argParser->parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        std::cerr << *_argParser;
        exit(EXIT_FAILURE);
    }
}

void ToolInit::createOutputDir() {
    _outputDirEnabled = true;

    if (FileUtils::exists(_outputsDir)) {
        if (!FileUtils::isDirectory(_outputsDir)) {
            spdlog::error("The directory {} is not a directory", _outputsDir);
            exit(EXIT_FAILURE);
            return;
        }
    } else {
        const bool createRes = FileUtils::createDirectory(_outputsDir);
        if (!createRes) {
            exit(EXIT_FAILURE);
            return;
        }
    }

    _outputsDir = std::filesystem::absolute(_outputsDir);
    _reportsDir = _outputsDir+"/reports";

    // Create outputs and reports directories
    std::filesystem::remove_all(_outputsDir);
    std::filesystem::remove_all(_reportsDir);
    std::filesystem::create_directory(_outputsDir);
    std::filesystem::create_directory(_reportsDir);
    
    const auto reportsPath = FileUtils::Path(_reportsDir);
    const auto logFilePath = reportsPath/(_toolName + ".log");
    _logFilePath = logFilePath.string();

    LogSetup::setupLogFileBacked(_logFilePath);

    // Init PerfStat
    PerfStat::init(reportsPath/(_toolName + ".perf"));
}

void ToolInit::resetDefaultGraph(const FileUtils::Path& graphsDir) {
    FileUtils::Path defaultGraphPath = graphsDir / "default";
    spdlog::info("Searching for default in {}", graphsDir.c_str());
    if (FileUtils::isDirectory(defaultGraphPath)) {
        FileUtils::removeDirectory(defaultGraphPath);
        spdlog::info("Default graph deleted.");
        return;
    }
    spdlog::warn("Default graph not found at {}", defaultGraphPath.c_str());
}

void ToolInit::init(int argc, const char** argv) {
    LogSetup::setupLogConsole();

    setupArgParser();

    parseArguments(argc, argv);

    // If the option is not present, defaults the output dir to toolname + .out
    if (_outputsDir.empty()) {
        const auto outDirPath =  std::filesystem::current_path()/(_toolName + ".out");
        _outputsDir = outDirPath.string();
    }

    if (_outputDirEnabled) {
        createOutputDir();
    }

    atexit(atexitHandler);
}

void ToolInit::printHelp() const {
    std::cout << '\n' <<  *_argParser;
}
