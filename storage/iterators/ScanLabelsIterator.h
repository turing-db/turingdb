#pragma once

#include "columns/ColumnVector.h"
#include "metadata/LabelMap.h"

namespace db {

class ScanLabelsIterator {
public:
    ScanLabelsIterator() = default;
    explicit ScanLabelsIterator(const LabelMap& labelMap);

    static ScanLabelsIterator end(const LabelMap& labelMap);

    void reset();
    void next();
    bool isValid() const { return _it != _end; }

    const LabelMap::Pair& get() const { return *_it; }
    LabelID getID() const { return _it->_id; }
    std::string_view getName() const { return *_it->_name; }

    const LabelMap::Pair& operator*() const { return *_it; }

    ScanLabelsIterator& operator++() {
        next();
        return *this;
    }

protected:
    LabelMap::Container::const_iterator _it;
    LabelMap::Container::const_iterator _end;

    ScanLabelsIterator(LabelMap::Container::const_iterator begin,
                       LabelMap::Container::const_iterator end)
        : _it(begin),
        _end(end)
    {
    }
};

class ScanLabelsChunkWriter : public ScanLabelsIterator {
public:
    ScanLabelsChunkWriter() = delete;
    explicit ScanLabelsChunkWriter(const LabelMap& labelMap);

    void setIDs(ColumnVector<LabelID>* ids) { _ids = ids; }
    void setNames(ColumnVector<std::string_view>* names) { _names = names; }

    void fill(size_t maxCount);

private:
    ColumnVector<LabelID>* _ids {nullptr};
    ColumnVector<std::string_view>* _names {nullptr};
};

struct ScanLabelsRange {
    const LabelMap& _labelMap;

    ScanLabelsIterator begin() const { return ScanLabelsIterator {_labelMap}; }
    ScanLabelsIterator end() const { return ScanLabelsIterator::end(_labelMap); }
    ScanLabelsChunkWriter chunkWriter() const { return ScanLabelsChunkWriter {_labelMap}; }
};

}
