#include "ChunkConfig.h"
#include "LogUtils.h"
#include "vmutils.h"

#include "Time.h"

using namespace db;

int main() {
    auto sample = VMSample::createSample(SAMPLE_NAME);

    if (!sample.loadJsonDB(sample._turingHome + "/neo4j/pole-db/")) {
        return 1;
    }

    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }
    if (!sample.executeFile(sample._sampleDir + "/program.turing")) {
        return 1;
    }

    sample.printOutput({"Name", "NodeID"}, 0, 20, 25);

    auto view = sample.readDB();
    ColumnVector<std::string_view> namesRaw;
    ColumnVector<EntityID> nodeIDsRaw;
    namesRaw.reserve(1369);
    nodeIDsRaw.reserve(1369);
    ColumnVector<std::string_view> names;
    ColumnVector<EntityID> nodeIDs;

    auto t0 = Clock::now();
    const auto displayNameType = view.getDB()->getMetadata()->propTypes().get("surname (String)");
    ScanNodePropertiesChunkWriter<types::String> chunkWriter(view.getDB(), displayNameType._id);
    chunkWriter.setPropertiesColumn(&namesRaw);
    chunkWriter.setNodeIDsColumn(&nodeIDsRaw);

    while (chunkWriter.isValid()) {
        chunkWriter.fill(ChunkConfig::CHUNK_SIZE);
    }

    for (size_t i = 0 ; i < names.size(); i++) {
        const auto& displayName = names[i];
        if (displayName == "Hamilton" || displayName == "Smith") {
            names.push_back(displayName);
            nodeIDs.push_back(nodeIDsRaw[i]);
        }
    }

    spdlog::info("Raw c++ performances with ChunkWriter:");
    logt::ElapsedTime(Milliseconds(Clock::now() - t0).count(), "ms");

    sample.destroy();

    return 0;
}
