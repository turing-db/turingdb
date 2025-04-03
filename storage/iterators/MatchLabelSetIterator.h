#pragma once

#include "Iterator.h"

#include "NodeRange.h"
#include "indexers/LabelSetIndexer.h"

namespace db {

class MatchLabelSetIterator : public Iterator {
public:
    MatchLabelSetIterator() = default;
    MatchLabelSetIterator(const GraphView& view, const LabelSetHandle& labelSet);
    ~MatchLabelSetIterator() override;

    bool isValid() const override {
        return _partIt.isValid();
    }

    void next() override;

    const LabelSetHandle& get() const {
        return _labelSetIt.getKey();
    }

    MatchLabelSetIterator& operator++() {
        next();
        return *this;
    }

    const LabelSetHandle& operator*() const { return get(); }

private:
    using LabelSetIterator = LabelSetIndexer<NodeRange>::MatchIterator;

    LabelSetHandle _targetLabelSet;
    LabelSetIterator _labelSetIt;
};

}
