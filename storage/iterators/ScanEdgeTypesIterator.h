#pragma once

#include "columns/ColumnVector.h"
#include "metadata/EdgeTypeMap.h"

namespace db {

class ScanEdgeTypesIterator {
public:
    ScanEdgeTypesIterator() = default;
    explicit ScanEdgeTypesIterator(const EdgeTypeMap& edgeTypeMap);

    static ScanEdgeTypesIterator end(const EdgeTypeMap& edgeTypeMap);

    void reset();
    void next();
    bool isValid() const { return _it != _end; }

    const EdgeTypeMap::Pair& get() const { return *_it; }
    EdgeTypeID getID() const { return _it->_id; }
    std::string_view getName() const { return *_it->_name; }

    const EdgeTypeMap::Pair& operator*() const { return *_it; }

    ScanEdgeTypesIterator& operator++() {
        next();
        return *this;
    }

protected:
    EdgeTypeMap::Container::const_iterator _it;
    EdgeTypeMap::Container::const_iterator _end;

    ScanEdgeTypesIterator(EdgeTypeMap::Container::const_iterator begin,
                       EdgeTypeMap::Container::const_iterator end)
        : _it(begin),
        _end(end)
    {
    }
};

class ScanEdgeTypesChunkWriter : public ScanEdgeTypesIterator {
public:
    ScanEdgeTypesChunkWriter() = delete;
    explicit ScanEdgeTypesChunkWriter(const EdgeTypeMap& edgeTypeMap);

    void setIDs(ColumnVector<EdgeTypeID>* ids) { _ids = ids; }
    void setNames(ColumnVector<std::string_view>* names) { _names = names; }

    void fill(size_t maxCount);

private:
    ColumnVector<EdgeTypeID>* _ids {nullptr};
    ColumnVector<std::string_view>* _names {nullptr};
};

struct ScanEdgeTypesRange {
    const EdgeTypeMap& _edgeTypeMap;

    ScanEdgeTypesIterator begin() const { return ScanEdgeTypesIterator {_edgeTypeMap}; }
    ScanEdgeTypesIterator end() const { return ScanEdgeTypesIterator::end(_edgeTypeMap); }
    ScanEdgeTypesChunkWriter chunkWriter() const { return ScanEdgeTypesChunkWriter {_edgeTypeMap}; }
};

}
