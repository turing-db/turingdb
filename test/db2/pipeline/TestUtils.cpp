#include "TestUtils.h"

#include "DBMetadata.h"
#include "DBAccess.h"
#include "DataBuffer.h"

using namespace db;

void TestUtils::generateMillionTestDB(DB& db, JobSystem& jobSystem) {
    const size_t nodeCount = 1000000;
    LabelSetMap& labelsets = db.getMetadata()->labelsets();

    std::unique_ptr<DataBuffer> buf = db.access().newDataBuffer();
    const LabelSetID labelsetID = labelsets.getOrCreate(LabelSet::fromList({0}));

    for (size_t i = 0; i < nodeCount; i++) {
        buf->addNode(labelsetID);
    }

    {
        auto datapart = db.uniqueAccess().createDataPart(std::move(buf));
        datapart->load(db.access(), jobSystem);
        db.uniqueAccess().pushDataPart(std::move(datapart));
    }
}
