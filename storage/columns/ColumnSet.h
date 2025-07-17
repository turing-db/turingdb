#pragma once

#include <unordered_set>

#include "BioAssert.h"
#include "Column.h"
#include "DebugDump.h"

namespace db {

template <typename T>
class ColumnSet : public Column {
public:
    using ValueType = T;
    using Iterator = std::unordered_set<ValueType>::iterator;
    using ConstIterator = std::unordered_set<ValueType>::const_iterator;

    static constexpr ColumnKind::ColumnKindCode BaseKind = static_cast<ColumnKind::ColumnKindCode>(ColumnKind::BaseColumnKind::SET);

    ColumnSet(const ColumnSet&) = default;
    ColumnSet(ColumnSet&&) noexcept = default;

    ColumnSet()
        : Column(_staticKind)
    {
    }

    ColumnSet(std::initializer_list<ValueType>&& v)
        : Column(_staticKind),
          _data(std::forward<std::initializer_list<ValueType>>(v))
    {
    }

    explicit ColumnSet(const std::unordered_set<ValueType>& set)
        : Column(_staticKind),
          _data(set)
    {
    }

    explicit ColumnSet(std::unordered_set<ValueType>&& set) noexcept
        : Column(_staticKind),
          _data(std::move(set))
    {
    }

    explicit ColumnSet(size_t size)
        : Column(_staticKind),
          _data(size)
    {
    }

    ~ColumnSet() override = default;

    ColumnSet& operator=(const ColumnSet&) = default;
    ColumnSet& operator=(ColumnSet&&) noexcept = default;

    bool empty() const { return _data.empty(); }
    size_t size() const override { return _data.size(); }


    Iterator begin() { return _data.begin(); }
    ConstIterator begin() const { return _data.begin(); }
    ConstIterator cbegin() const { return _data.cbegin(); }

    void assign(const Column* other) override {
        const ColumnSet<ValueType>* otherCol = dynamic_cast<const ColumnSet<ValueType>*>(other);
        msgbioassert(otherCol, "ColumnSet::assign: other is not a ColumnSet of the same type");
        _data = otherCol->_data;
    }

    void clear() noexcept { _data.clear(); }

    std::pair<Iterator, bool> insert(const ValueType& val) { return _data.insert(val); }
    std::pair<Iterator, bool> insert(ValueType&& val) { return _data.insert(val); }

    template <typename... Args>
    std::pair<Iterator, bool> emplace(Args&&... args) {
        return _data.emplace(std::forward<Args>(args)...);
    }

    //TODO: Iterator hinted insertions and emplacings

    Iterator erase(Iterator pos) { return _data.erase(pos); }
    ConstIterator erase(ConstIterator cpos) { return _data.erase(cpos); }
    size_t erase(const ValueType& key) { return _data.erase(key); }

    void swap(ColumnSet& other) noexcept { _data.swap(other._data); }

    size_t count(const ValueType& key) const { return _data.count(key); }
    // NOTE: C++20 templated version not added here

    Iterator find(const ValueType& key) { return _data.find(key); }
    ConstIterator find(const ValueType& key) const { return _data.find(key); }
    // NOTE: C++20 templated versions not added here

    bool contains(const ValueType& key) const { return _data.contains(key); }
    // NOTE: C++20 templated version not added here

    void dump() const override {
        DebugDump::dumpString("ColumnSet of size=" + std::to_string(size()));
        std::ranges::for_each(_data, DebugDump::dump);
    }

    std::unordered_set<ValueType>& getRaw() { return _data; }
    const std::unordered_set<ValueType>& getRaw() const { return _data; }

    static consteval auto staticKind() { return _staticKind; }

private:
    std::unordered_set<ValueType> _data;

    static constexpr auto _staticKind = ColumnKind::getColumnKind<ColumnSet<ValueType>>();
};
}

