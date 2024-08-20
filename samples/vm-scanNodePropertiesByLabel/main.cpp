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
    sample.printOutput({"Name"}, 0, 30);

    sample.destroy();

    return 0;
}
