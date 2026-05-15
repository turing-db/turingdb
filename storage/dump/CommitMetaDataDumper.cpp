#include "CommitMetaDataDumper.h"

#include <range/v3/view/enumerate.hpp>

#include "datapart/DataPart.h"

#include "dump/GraphDumpHelper.h"
#include "versioning/Commit.h"
#include "datapart/DataPartSpan.h"

using namespace db;

DumpResult<void> CommitMetaDataDumper::dump(const Commit& commit,
                                            fs::FileWriter<>& writer) {
    GraphDumpHelper::writeFileHeader(&writer);

    writer.write(commit.getNumNodes());
    writer.write(commit.getNumEdges());

    const DataPartSpan commitDataParts = commit.data().commitDataparts();
    const size_t commitDataSize = commitDataParts.size();
    writer.write(commitDataSize);

    const DataPartSpan allDataParts = commit.data().allDataparts();
    const size_t numDataParts = allDataParts.size();

    writer.write(numDataParts);
    for (const auto& dp : allDataParts) {
        writer.write(dp->getID().get());
    }


    return {};
}
