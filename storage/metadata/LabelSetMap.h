#pragma once

#include <optional>
#include <unordered_map>

#include "ID.h"
#include "LabelSetHandle.h"

namespace db {

class LabelSetMap {
public:
    struct Pair {
        LabelSetID _id;
        std::unique_ptr<LabelSet> _value;
    };

    using Container = std::vector<Pair>;
    using ValueMap = std::unordered_map<LabelSetHandle, size_t, LabelSetHandle::Hash, LabelSetHandle::Equal>;
    using IDMap = std::unordered_map<LabelSetID, size_t>;

    LabelSetMap();
    ~LabelSetMap();

    LabelSetMap(const LabelSetMap&);
    LabelSetMap(LabelSetMap&&) noexcept = default;
    LabelSetMap& operator=(const LabelSetMap&);
    LabelSetMap& operator=(LabelSetMap&&) noexcept = default;

    [[nodiscard]] std::optional<LabelSetID> get(const LabelSet& labelset) const;
    [[nodiscard]] std::optional<LabelSetID> get(const LabelSetHandle& labelsetRef) const;
    [[nodiscard]] std::optional<LabelSetHandle> getValue(LabelSetID id) const;
    [[nodiscard]] size_t getCount() const;

    [[nodiscard]] Container::const_iterator begin() const { return _container.begin(); }
    [[nodiscard]] Container::const_iterator end() const { return _container.end(); }

    LabelSetHandle getOrCreate(const LabelSet& labelset);

private:
    Container _container;
    ValueMap _valueMap;
    IDMap _idMap;
};

}

