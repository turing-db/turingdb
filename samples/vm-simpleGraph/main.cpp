#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    sample.createSimpleGraph();

    if (!sample.generateFromFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }

    sample.execute();
    sample.printOutput({"Src", "Tgt1", "Tgt2", "Tgt3"});

    sample.destroy();

    return 0;
}
