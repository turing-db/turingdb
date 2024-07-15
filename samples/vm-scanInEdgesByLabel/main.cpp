#include <spdlog/spdlog.h>

#include "Assembler.h"
#include "DB.h"
#include "DBAccess.h"
#include "DataEnv.h"
#include "LogSetup.h"
#include "LogUtils.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"
#include "PerfStat.h"
#include "Program.h"
#include "ScanNodesIterator.h"
#include "SystemManager.h"
#include "Time.h"
#include "VM.h"

using namespace db;

int main() {
    LogSetup::setupLogFileBacked(SAMPLE_NAME ".log");
    PerfStat::init(SAMPLE_NAME ".perf");
    spdlog::set_level(spdlog::level::info);
    const std::string turingHome = std::getenv("TURING_HOME");
    const std::string sampleDir = turingHome + "/samples/" SAMPLE_NAME;
    Program program;

    JobSystem jobSystem;
    jobSystem.initialize();

    // Create assembler
    Assembler assembler;

    // Initialize system
    auto system = std::make_unique<SystemManager>();

    // Load db
    Neo4jImporter::importJsonDir(
        jobSystem,
        system->getDefaultDB(),
        nodeCountLimit,
        edgeCountLimit,
        Neo4jImporter::ImportJsonDirArgs {
            ._jsonDir = turingHome + "/neo4j/pole-db/",
        });

    // Initialize VM
    VM vm(system.get());


    // Compile & execute program
    spdlog::info("== Code Generation ==");
    auto t0 = Clock::now();

    if (!assembler.generateFromFile(program, sampleDir + "/program.turing")) {
        spdlog::error("Error program invalid");
        return 1;
    }

    logt::ElapsedTime(Microseconds(Clock::now() - t0).count(), "us");

    // Initialize VM
    spdlog::info("== Init VM ==");
    t0 = Clock::now();

    vm.initialize();

    logt::ElapsedTime(Microseconds(Clock::now() - t0).count(), "us");

    // Execution
    spdlog::info("== Execution ==");
    t0 = Clock::now();

    vm.exec(&program);
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    spdlog::info("Output:");
    const auto& output = vm.readRegister<OutputWriter>(0)->getResult();
    std::string str;
    for (size_t i = 0; i < output[0].size(); i++) {
        for (size_t j = 0; j < output.size(); j++) {
            str += fmt::format("{:>7}", output[j][i]);
        }
        if (i == 20) {
            break;
        }
        str += '\n';
    }
    spdlog::info("\n{}\n...", str);

    PerfStat::destroy();
    return 0;
}
