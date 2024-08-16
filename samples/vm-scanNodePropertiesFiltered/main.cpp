#include "LogUtils.h"
#include "vmutils.h"

#include "Time.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    // if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
    if (!sample.loadJsonDB("/home/dev/json-dbs/reactome-json/")) {
        return 1;
    }

    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }
    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }

    sample.printOutput({"Name", "NodeID"});

    auto access = sample.readDB();
    auto t0 = Clock::now();
    std::vector<std::string_view> names;
    const auto displayNameType = access.getDB()->getMetadata()->propTypes().get("displayName (String)");
    for (const auto& displayName : access.scanNodeProperties<types::String>(displayNameType._id)) {
        if (displayName == "APOE-4 [extracellular region]" || displayName == "AKT1 [plasma membrane]") {
            names.push_back(displayName);
        }
    }
    spdlog::info(names.size());
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    sample.destroy();

    return 0;
}
