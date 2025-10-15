#include "CommitHistoryBuilder.h"

#include "CommitHistory.h"

using namespace db;

void CommitHistoryBuilder::addDatapart(const WeakArc<DataPart>& datapart) {
        _history._allDataparts.push_back(datapart);
}

void CommitHistoryBuilder::setCommitDatapartCount(size_t count) {
    WeakArc<DataPart>* begin = _history._allDataparts.data();
    const size_t totalCount = _history._allDataparts.size();
    const size_t offset = totalCount - count;
    WeakArc<DataPart>* ptr = begin + offset;

    _history._commitDataparts = {
        ptr,
        count,
    };
}
