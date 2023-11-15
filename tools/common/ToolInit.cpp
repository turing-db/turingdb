#include "ToolInit.h"

#include "FileUtils.h"
#include "BannerDisplay.h"

#include "BioLog.h"
#include "PerfStat.h"
#include "MsgCommon.h"

using namespace Log;

ToolInit::ToolInit(const std::string& toolName)
    : _toolName(toolName),
    _argParser(toolName)
{
}

ToolInit::~ToolInit() {
}

void ToolInit::init(int argc, const char** argv) {
    BioLog::init();

    // Option to change the output directory
    _argParser.addOption(
        "o",
        "Changes the default output directory",
        "output_dir");
    _argParser.parse(argc, argv);

    for (const auto& option : _argParser.options()) {
        const auto& optName = option.first;

        if (optName == "o") {
            _outputsDir = option.second;
            break;
        }
    }

    // If the option is not present, defaults the output dir to toolname + .out
    if (_outputsDir.empty()) {
        _outputsDir = std::filesystem::current_path()/(_toolName + ".out");
    }

    if (FileUtils::exists(_outputsDir)) {
        if (!FileUtils::isDirectory(_outputsDir)) {
            BioLog::log(msg::ERROR_NOT_A_DIRECTORY() << _outputsDir.string());
            exit(EXIT_FAILURE);
            return;
        }
    } else {
        const bool createRes = FileUtils::createDirectory(_outputsDir);
        if (!createRes) {
            BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << _outputsDir.string());
            exit(EXIT_FAILURE);
            return;
        }
    }

    _outputsDir = std::filesystem::absolute(_outputsDir);
    _reportsDir = _outputsDir/"reports";

    // Create outputs and reports directories
    std::filesystem::remove_all(_outputsDir);
    std::filesystem::remove_all(_reportsDir);
    std::filesystem::create_directory(_outputsDir);
    std::filesystem::create_directory(_reportsDir);

    // Init logging
    const auto logFilePath = _reportsDir/(_toolName + ".log");
    BioLog::openFile(logFilePath.string());
    BioLog::echo(BannerDisplay::getBannerString());

    // Init PerfStat
    PerfStat::init(_reportsDir/(_toolName + ".perf"));
}
