#include "vmutils.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    sample.createSimpleGraph();
    //if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
    //if (!sample.loadJsonDB("/home/dev/json-dbs/reactome-json")) {
    //    return 1;
    //}

    if (!sample.generateFromFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }

    sample.execute();
    sample.printOutputProperty("name", {
        "  (n1)  ",
        //"  [EdgeID  :",
        //"EdgeType]",
        "->(n2)  ",
        //"  [EdgeID  :",
        //"EdgeType]",
        "->(n3)  ",
    }, 0, 20);
    sample.destroy();

    for (const auto& p : sample.readDB().scanNodeProperties<types::String>(0)) {
        spdlog::info(p);
    }

    return 0;
}
