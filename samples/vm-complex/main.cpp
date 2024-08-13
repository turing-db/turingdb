#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
        return 1;
    }

    const EntityID nodeID = sample.findNode("nhs_no (String)", "117-66-8129");
    if (!nodeID.isValid()) {
        return 1;
    }

    spdlog::info("Found node at index {}", nodeID);

    ColumnIDs ids {nodeID};
    sample._vm->setRegisterValue(1, &ids);

    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }
    sample._vm->setRegisterValue(1, &ids);

    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }

    sample.printOutput({"InputID", "TgtID_2"});

    sample.destroy();

    return 0;
}
