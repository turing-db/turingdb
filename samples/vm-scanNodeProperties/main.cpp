#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    sample.createSimpleGraph();
    //if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
    //    return 1;
    //}

    if (!sample.generateFromFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }

    sample.execute();
    //sample.printOutput({
    //    "SrcID",
    //    "EdgeID",
    //    "EdgeType",
    //    "TgtID",
    //});
    const auto* props = sample._vm->readRegister<ColumnVector<std::string>>(2);
    for (const auto& prop : *props) {
        spdlog::info(prop);
    }


    sample.destroy();

    return 0;
}
