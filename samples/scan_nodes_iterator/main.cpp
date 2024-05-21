#include <memory>
#include <chrono>
#include <iostream>

#include "DB.h"
#include "DBAccess.h"
#include "Reader.h"
#include "ScanNodesIterator.h"
#include "DataBuffer.h"
#include "ChunkConfig.h"

using namespace db;

using Clock = std::chrono::system_clock;

[[gnu::noinline]]
void printDuration(const char* title, std::chrono::duration<double, std::milli> duration) {
    std::cout << title << " " << duration.count() << "ms\n";
}

bool run() {
    const size_t nodeCount = 100ull*1000000;

    auto db = std::make_unique<DB>();
    auto access = db->uniqueAccess();

    const auto timeStart = Clock::now();

    {
        DataBuffer buf = access.newDataBuffer();
        for (size_t i = 0; i < nodeCount; i++) {
            buf.addNode({0});
        } 

        access.pushDataPart(buf);
    } 

    const auto partCreation = Clock::now();

    auto reader = access.getReader();
    auto it = reader.scanNodes().begin();

    // Read node by node
    size_t count = 0;
    for (; it.isValid(); ++it) {
        count++;
    }

    if (nodeCount != count) {
        return false;
    }

    const auto nodeByNodeRead = Clock::now();

    ColumnNodes colNodes;
    colNodes.reserve(ChunkConfig::CHUNK_SIZE);

    const auto chunkReadStart = Clock::now();

    // Read nodes by chunks
    it = reader.scanNodes().begin();
    count = 0;
    size_t iterations = 0;
    while (it.isValid()) {
        iterations++;
        it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);

        for (EntityID id : colNodes) {
            (void)id;
            count++;
        }
    }

    if (count != nodeCount) {
        return false;
    }

    const auto timeEnd = Clock::now();
    const auto totalDuration = timeEnd - timeStart;

    std::cout << "Chunk iterations " << iterations << "\n";

    printDuration("Part creation", partCreation-timeStart);
    printDuration("Node by node iteration", nodeByNodeRead-partCreation);
    printDuration("Chunk by chunk iteration", timeEnd-chunkReadStart);
    printDuration("Total", totalDuration);

    return true;
}

int main() {
    run();
    return 0;
}
