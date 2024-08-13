#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    sample.createSimpleGraph();
    // if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
    // if (!sample.loadJsonDB("/home/dev/json-dbs/reactome-json")) {
    //     return 1;
    // }

    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }
    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }
    sample.printOutput({"  (n1)  ",
                        "->(n2)  ",
                        "->(n3)  "},
                       0, 20);
    sample.destroy();

    return 0;
}
