#include "TestUtils.h"

#include "DBAccess.h"
#include "DataBuffer.h"

using namespace db;

void TestUtils::generateMillionTestDB(DB& db, JobSystem& jobSystem) {
    const size_t nodeCount = 1000000;
    LabelsetMap& labelsets = db.metaData()->labelsets();

    std::unique_ptr<DataBuffer> buf = db.access().newDataBuffer();
    const LabelsetID labelsetID = labelsets.getOrCreate(Labelset::fromList({0}));

    for (size_t i = 0; i < nodeCount; i++) {
        buf->addNode(labelsetID);
    }

    {
        auto datapart = db.uniqueAccess().prepareNewDataPart(std::move(buf));
        db.access().loadDataPart(*datapart, jobSystem);
        db.uniqueAccess().pushDataPart(std::move(datapart));
    }
}
