#include "TuringServerComponent.h"

#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <boost/process.hpp>

#include "ToolInit.h"
#include "BannerDisplay.h"
#include "FileUtils.h"

TuringServerComponent::TuringServerComponent(ToolInit& toolInit)
    : TuringToolComponent(toolInit),
    _serverCommand("server"),
    _serverStartCommand("start"),
    _serverStopCommand("stop")
{
}

TuringServerComponent::~TuringServerComponent() {
}

void TuringServerComponent::setup() {
    auto& argParser = _toolInit.getArgParser();

    _serverCommand.add_description("Manage Turing server");

    // turing server start
    _serverStartCommand.add_description("Start Turing server");
    _serverStartCommand.add_argument("--prototype")
                       .store_into(_isPrototypeMode);
    _serverCommand.add_subparser(_serverStartCommand);

    // turing server stop
    _serverStopCommand.add_description("Stop Turing server");
    _serverCommand.add_subparser(_serverStopCommand);

    argParser.add_subparser(_serverCommand);
}

bool TuringServerComponent::isActive() const {
    const auto& argParser = _toolInit.getArgParser();
    return argParser.is_subcommand_used("server");
}

void TuringServerComponent::run() {
    _toolInit.createOutputDir();
    BannerDisplay::printBanner();

    const auto turingAppPath = boost::process::search_path("turing-app");
    if (turingAppPath.empty()) {
        spdlog::error("The command turing-app could not be found, please check your turing installation");
        return;
    }

    boost::process::child appProcess(
        turingAppPath,
        boost::process::start_dir(_toolInit.getOutputsDir()));

    appProcess.detach();
    
    // Write process id in output directory
    /*
    const auto pid = appProcess.id();
    const std::string pidStr = std::to_string(pid)+"\n";
    const auto pidFile = _toolInit.getOutputsDir()+"/turing-app.pid";
    if (!FileUtils::writeFile(pidFile, pidStr)) {
        spdlog::error("Failed to write {} for turing-app process",
                      pidFile);
    }
    */
}
