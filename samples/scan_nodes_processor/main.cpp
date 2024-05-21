#include <memory>
#include <chrono>
#include <iostream>

#include "DB.h"
#include "DBAccess.h"
#include "DataBuffer.h"
#include "ScanNodesProcessor.h"
#include "DiscardProcessor.h"
#include "Pipeline.h"
#include "PipelineExecutor.h"
#include "ScanNodesIterator.h"
#include "ColumnNodes.h"
#include "ChunkConfig.h"

using namespace db;

using Clock = std::chrono::system_clock;

[[gnu::noinline]]
void printDuration(const char* title, std::chrono::duration<double, std::milli> duration) {
    std::cout << title << " " << duration.count() << "ms\n";
}

[[gnu::noinline]]
void chunkTest(DB* db) {
    ColumnNodes* col = new ColumnNodes();
    col->reserve(ChunkConfig::CHUNK_SIZE);

    ScanNodesIterator it(db);

    while (it.isValid()) {
        it.fill(col, ChunkConfig::CHUNK_SIZE);
    }
}

bool run() {
    const size_t nodeCount = 100ul*1000000;

    auto db = std::make_unique<DB>();
    auto access = db->uniqueAccess();

    // Part creation
    const auto timeStart = Clock::now();

    {
        DataBuffer buf = access.newDataBuffer();
        for (size_t i = 0; i < nodeCount; i++) {
            buf.addNode({0});
        }

        access.pushDataPart(buf);
    } 

    const auto partCreation = Clock::now();

    // Pipeline construction
    Pipeline pipe;
    auto* scanNodes = pipe.addProcessor<ScanNodesProcessor>(db.get());
    auto* scanNodesOut = scanNodes->createOut(&scanNodes->writeNodeID);
    auto* discard = pipe.addProcessor<DiscardProcessor>();
    discard->setInput(&discard->inputData, scanNodesOut);

    ColumnNodes* myCol = new ColumnNodes();
    myCol->reserve(ChunkConfig::CHUNK_SIZE);

    PipelineExecutor pipeExec(&pipe);

    const auto execStartTime = Clock::now();
    pipeExec.run();
    const auto execEnd = Clock::now();

    const auto itBegin = Clock::now();
    chunkTest(db.get());
    const auto itEnd = Clock::now();

    // Statistics
    const auto timeEnd = Clock::now();
    const auto totalDuration = timeEnd - timeStart;

    printDuration("Part creation", partCreation-timeStart);
    printDuration("Pipeline test", execEnd-execStartTime);
    printDuration("Chunk test", itEnd-itBegin);
    printDuration("Total", totalDuration);

    return true;
}

int main() {
    run();
    return 0;
}
