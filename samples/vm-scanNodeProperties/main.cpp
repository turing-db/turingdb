#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    sample.createSimpleGraph();

    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }
    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }
    sample.printOutput({"Name"});

    sample.destroy();

    return 0;
}
