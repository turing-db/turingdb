#pragma once

#include <string_view>
#include <unordered_set>

namespace db {

// The properties a JSONL import reads as instants. A datetime arrives as a plain JSON
// string, which a string property is spelled as too, so the import is told which names
// carry one rather than guessing from what the text looks like.
class DateTimeSpec {
public:
    using SetType = std::unordered_set<std::string_view>;
    using ConstIterator = SetType::const_iterator;

    bool contains(std::string_view propName) const;

    ConstIterator begin() const { return _names.begin(); };
    ConstIterator end() const { return _names.end(); };

    bool empty() const { return _names.empty(); }

    void emplace(std::string_view name) { _names.emplace(name); }

private:
    SetType _names;
};

}
