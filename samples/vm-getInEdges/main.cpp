#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
        return 1;
    }

    if (!sample.generateFromFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }

    sample.execute();
    sample.printOutput({
        "TgtID_1",
        "EdgeID_1",
        "EdgeType_1",
        "SrcID_1",
        "EdgeID_2",
        "EdgeType_2",
        "SrcID_2",
    }, 0, 10);
    sample.destroy();

    return 0;
}
