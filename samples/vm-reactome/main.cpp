#include <spdlog/spdlog.h>

#include "Assembler.h"
#include "DB.h"
#include "GraphView.h"
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
    Program program;

    JobSystem jobSystem;
    jobSystem.initialize();

    // Create assembler
    Assembler assembler;

    // Initialize system
    auto system = std::make_unique<SystemManager>();
    auto* db = system->getDefaultDB();
    const DBMetadata* metadata = db->getMetadata();
    const PropertyTypeMap& propTypes = metadata->propTypes();
    const LabelMap& labels = metadata->labels();

    // Load db
    if (!Neo4jImporter::importJsonDir(
            jobSystem,
            db,
            nodeCountLimit,
            edgeCountLimit,
            Neo4jImporter::ImportJsonDirArgs {
                ._jsonDir = "/home/dev/json-dbs/reactome-json",
            })) {
        return 1;
    }

    auto access = db->access();
    std::string_view apoe4ReactomeID = "R-HSA-9711070";
    PropertyType stIDType = propTypes.get("stId (String)");

    const auto findReactomeID = [&]() {
        LabelSet labelset;
        labelset.set(labels.get("DatabaseObject"));
        labelset.set(labels.get("PhysicalEntity"));
        labelset.set(labels.get("GenomeEncodedEntity"));
        labelset.set(labels.get("EntityWithAccessionedSequence"));

        auto it = access.scanNodePropertiesByLabel<types::String>(stIDType._id, &labelset).begin();
        size_t i = 0;
        for (; it.isValid(); it.next()) {
            const types::String::Primitive& stID = it.get();
            if (stID == apoe4ReactomeID) {
                EntityID apoeID = it.getCurrentNodeID();
                spdlog::info("Found APOE-4 at ID: {} in {} string comparisons", apoeID.getValue(), i);
                return apoeID;
            }
            i++;
        }
        spdlog::error("Could not find APOE-4");
        return EntityID {};
    };

    auto t0 = Clock::now();
    spdlog::info("== Finding APOE id ==");
    const EntityID apoeID = findReactomeID();
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    // Initialize VM
    VM vm(system.get());

    // Compile & execute program
    spdlog::info("== Code Generation ==");
    t0 = Clock::now();

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

    ColumnIDs inputIDs {apoeID};
    vm.setRegisterValue(1, &inputIDs);
    vm.exec(&program);
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    spdlog::info("Output:");
    const auto& output = vm.readRegister<OutputWriter>(0)->getResult();
    std::string str;
    str += fmt::format("{:>24}", "SrcID_1");
    str += fmt::format("{:>24}", "EdgeType_1");
    str += fmt::format("{:>24}", "EdgeID_1");
    str += fmt::format("{:>24}", "TgtID_1");
    str += fmt::format("{:>24}", "EdgeType_2");
    str += fmt::format("{:>24}", "EdgeID_2");
    str += fmt::format("{:>24}", "TgtID_2");
    str += "\n";
    for (size_t i = 0; i < output[0].size(); i++) {
        for (size_t j = 0; j < output.size(); j++) {
            str += fmt::format("{:>24}", output[j][i]);
        }
        //if (i == 20) {
        //    str += "\n...";
        //    break;
        //}
        str += '\n';
    }
    spdlog::info("\n{}", str);
    spdlog::info("NLines: {}", output[0].size());

    PerfStat::destroy();
    return 0;
}
