#include <range/v3/view/zip.hpp>
#include <range/v3/view/take.hpp>
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
#include "ScanEdgesIterator.h"
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
            ._jsonDir = turingHome + "/neo4j/cyber-security-db/",
        });

    // Initialize VM
    VM vm(system.get());


    // Compile program
    spdlog::info("== Compilation ==");
    auto t0 = Clock::now();

    auto program = assembler.generateFromFile(sampleDir + "/program.turing");
    if (program->size() == 0) {
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
    // Reserve a register for the output (node IDs)
    env.addEntityIDColumn(0, ColumnIDs {});

    // Execution
    spdlog::info("== Execution ==");
    t0 = Clock::now();

    vm.exec(program.get(), &env);
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    const auto& nodeIDs = *vm.readRegister<ColumnIDs>(0);

    std::string output;
    for (const EntityID nodeID : nodeIDs | ranges::views::take(10)) {
        output += std::to_string(nodeID) + ' ';
    }
    spdlog::info("First 10 nodes: {}", output);

    // for (const auto& e : system->getDefaultDB()->access().scanOutEdges()) {
    //     spdlog::info("Edge {}: [{}->{}]", e._edgeID, e._nodeID, e._otherID);
    // }

    PerfStat::destroy();
    return 0;
}
