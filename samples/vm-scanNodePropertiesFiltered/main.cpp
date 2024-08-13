#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    //if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
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

    sample.destroy();

    return 0;
}
