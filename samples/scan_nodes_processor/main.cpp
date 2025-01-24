#include <chrono>
#include <iostream>
#include <memory>

#include "ChunkConfig.h"
#include "ColumnIDs.h"
#include "DB.h"
#include "GraphView.h"
#include "DataPartBuilder.h"
#include "DiscardProcessor.h"
#include "JobSystem.h"
#include "Pipeline.h"
#include "PipelineExecutor.h"
#include "ScanNodesIterator.h"
#include "ScanNodesProcessor.h"

using namespace db;
using namespace js;

using Clock = std::chrono::system_clock;

[[gnu::noinline]]
void printDuration(const char* title, std::chrono::duration<double, std::milli> duration) {
    std::cout << title << " " << duration.count() << "ms\n";
}

[[gnu::noinline]]
void chunkTest(DB* db) {
    ColumnIDs* col = new ColumnIDs();
    col->reserve(ChunkConfig::CHUNK_SIZE);

    ScanNodesIterator it(db);

    while (it.isValid()) {
        it.fill(col, ChunkConfig::CHUNK_SIZE);
    }
}

bool run() {
    const size_t nodeCount = 100'000'000ull;

    auto db = std::make_unique<DB>();
    JobSystem jobSystem;
    jobSystem.initialize();

    // Part creation
    const auto timeStart = Clock::now();

    {
        auto buf = db->view().createDataPart();
        for (size_t i = 0; i < nodeCount; i++) {
            buf->addNode(LabelSetID{0});
        }

        {
            auto datapart = db->uniqueAccess().createDataPart(std::move(buf));
            datapart->load(db->view(), jobSystem);
            db->uniqueAccess().pushDataPart(std::move(datapart));
        }
    }

    const auto partCreation = Clock::now();

    // Pipeline construction
    Pipeline pipe;
    auto* scanNodes = pipe.addProcessor<ScanNodesProcessor>(db.get());
    auto* scanNodesOut = scanNodes->createOut(&scanNodes->writeNodeID);
    auto* discard = pipe.addProcessor<DiscardProcessor>();
    discard->setInput(&discard->inputData, scanNodesOut);

    ColumnIDs* myCol = new ColumnIDs();
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

    printDuration("Part creation", partCreation - timeStart);
    printDuration("Pipeline test", execEnd - execStartTime);
    printDuration("Chunk test", itEnd - itBegin);
    printDuration("Total", totalDuration);

    jobSystem.terminate();
    return true;
}

int main() {
    run();
    return 0;
}
