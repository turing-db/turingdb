#pragma once

#include <optional>
#include <unordered_map>
#include <string>

#include "ID.h"

namespace db {

class LabelMap {
public:
    struct Pair {
        LabelID _id;
        std::unique_ptr<std::string> _name;
    };

    using Container = std::vector<Pair>;
    using NameMap = std::unordered_map<std::string_view, size_t>;
    using IDMap = std::unordered_map<LabelID, size_t>;

    LabelMap();
    ~LabelMap();

    LabelMap(const LabelMap&);
    LabelMap(LabelMap&&) noexcept = default;
    LabelMap& operator=(const LabelMap&);
    LabelMap& operator=(LabelMap&&) noexcept = default;

    [[nodiscard]] std::optional<LabelID> get(std::string_view name) const;
    [[nodiscard]] std::optional<std::string_view> getName(LabelID id) const;
    [[nodiscard]] size_t getCount() const;

    [[nodiscard]] Container::const_iterator begin() const { return _container.begin(); }
    [[nodiscard]] Container::const_iterator end() const { return _container.end(); }

    LabelID getOrCreate(const std::string& labelName);

private:
    Container _container;
    NameMap _nameMap;
    IDMap _idMap;
};

}
