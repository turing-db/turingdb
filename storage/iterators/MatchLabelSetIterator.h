#pragma once

#include "Iterator.h"

#include "NodeRange.h"
#include "indexers/LabelSetIndexer.h"

namespace db {

class MatchLabelSetIterator : public Iterator {
public:
    MatchLabelSetIterator() = default;
    MatchLabelSetIterator(const GraphView& view, const LabelSet* labelSet);
    ~MatchLabelSetIterator() override;

    bool isValid() const override {
        return _partIt.isValid();
    }

    void next() override;

    LabelSetID get() const {
        return _labelSetIt.getID();
    }

    MatchLabelSetIterator& operator++() {
        next();
        return *this;
    }

    LabelSetID operator*() const { return get(); }

private:
    using LabelSetIterator = LabelSetIndexer<NodeRange>::MatchIterator;

    const LabelSet* _targetLabelSet {nullptr};
    LabelSetIterator _labelSetIt;
};

}
