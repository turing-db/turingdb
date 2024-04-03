#include "TestUtils.h"

#include "DBAccess.h"
#include "TempBuffer.h"

using namespace db;

void TestUtils::generateMillionTestDB(DBAccess& acc) {
    const size_t nodeCount = 1000000;

    TempBuffer buf = acc.createTempBuffer();
    for (size_t i = 0; i < nodeCount; i++) {
        buf.addNode({0});
    }

    acc.pushDataPart(buf);
}
