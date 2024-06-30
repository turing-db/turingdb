#include <range/v3/view/zip.hpp>
#include <spdlog/spdlog.h>

#include "Assembler.h"
#include "DataEnv.h"
#include "LogSetup.h"
#include "LogUtils.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"
#include "PerfStat.h"
#include "Program.h"
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
    spdlog::info("== Compilation ==");
    auto t0 = Clock::now();

    auto program = assembler.generateFromFile(sampleDir + "/program.turing");
    if (program->size() == 0) {
        spdlog::error("Error program invalid");
        return 1;
    }

    logt::ElapsedTime(Microseconds(Clock::now() - t0).count(), "us");


    // Initialize VM
    spdlog::info("== Init VM ==");
    t0 = Clock::now();

    vm.initialize();

    logt::ElapsedTime(Microseconds(Clock::now() - t0).count(), "us");


    // Setup DataEnv (initial conditions for registers
    DataEnv env;
    env.addEntityIDColumn(0, {});
    env.addEntityIDColumn(1, {});
    env.addEntityIDColumn(2, {});
    env.addIndices(3, {});

    // Execution
    spdlog::info("== Execution ==");
    t0 = Clock::now();

    vm.exec(program.get(), &env);
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    spdlog::info("First 10 edges:");
    const auto& output = vm.readRegister<OutputWriter>(4)->getResult();
    std::string str;
    for (size_t i = 0; i < 10; i++) {
        for (size_t j = 0; j < output.size(); j++) {
            str += fmt::format("{:>7}", output[j][i]);
        }
        str += '\n';
    }
    spdlog::info("\n{}", str);

    PerfStat::destroy();
    return 0;
}
