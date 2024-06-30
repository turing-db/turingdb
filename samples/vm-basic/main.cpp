#include <spdlog/spdlog.h>

#include "Assembler.h"
#include "DataEnv.h"
#include "LogSetup.h"
#include "LogUtils.h"
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

    // Create assembler
    Assembler assembler;

    // Initialize system
    auto system = std::make_unique<SystemManager>();

    // Initialize VM
    VM vm(system.get());


    // Assemble program
    spdlog::info("== Assembly ==");
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


    // Setup DataEnv (No initial conditions in this example)
    DataEnv env;

    // Execution
    spdlog::info("== Execution ==");
    t0 = Clock::now();

    vm.exec(program.get(), &env);
    spdlog::info("Sum: {}", vm.readRegister(0));
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    PerfStat::destroy();
    return 0;
}

/* For GetOutEdges:
 *
 * during iteration, for each entry, store the update the repr history
 * Ex: Input {0, 2, 4}:      [0:0->1] [1:0->2] [2:2->2] [3:4->2] [4:4->3]
 * 1) [0:1] [1:0] [2:0]
 * 2) [0:2] [1:0] [2:0]
 * 3) [0:2] [1:1] [2:0]
 * 4) [0:2] [1:1] [2:1]
 * 5) [0:2] [1:1] [2:2]
 *
 *
 * it1          it2         it3
 * 0{0}1        1{1}2       2{3}4
 * 0{0}8        1{2}3       2{4}5
 *              8{7}9       2{5}6
 *                          3{6}7
 *
 * {2}          {2, 1}      {3, 1, 0}
 * {0 0}        {1 1 8}     {2 2 2 3}
 * {4}          {4, 0}      {3, 1, 0}
 *
 *
 *
 * Output:
 * 0 1 2 4
 * 0 1 2 5
 * 0 1 2 6
 * 0 1 3 7
 *
 *
 * */
