#pragma once

#include <string_view>
#include <unordered_set>

namespace db {

// The properties an import reads as durations. Neither JSONL nor Parquet has a duration
// type, so a duration arrives as a plain integer count of microseconds and the import is
// told which names carry one.
class DurationSpec {
public:
    using SetType = std::unordered_set<std::string_view>;
    using ConstIterator = SetType::const_iterator;

    bool contains(std::string_view propertyName) const;

    ConstIterator begin() const { return _names.begin(); };
    ConstIterator end() const { return _names.end(); };

    bool empty() const { return _names.empty(); }

    void emplace(std::string_view name) { _names.emplace(name); }

private:
    SetType _names;
};

}
