#pragma once

#include <unordered_set>

#include "BioAssert.h"
#include "Column.h"
#include "DebugDump.h"
#include "FatalException.h"

namespace db {

template <typename T>
class ColumnSet : public Column {
public:
    using ValueType = T;
    using Iterator = std::unordered_set<T>::iterator;
    using ConstIterator = std::unordered_set<T>::const_iterator;

    static constexpr ColumnKind::ColumnKindCode BaseKind = (ColumnKind::ColumnKindCode)ColumnKind::BaseColumnKind::SET;

    ColumnSet(const ColumnSet&) = default;
    ColumnSet(ColumnSet&&) noexcept = default;

    ColumnSet()
        : Column(_staticKind)
    {
    }

    ColumnSet(std::initializer_list<T>&& v)
        : Column(_staticKind),
          _data(std::forward<std::initializer_list<T>>(v))
    {
    }

    explicit ColumnSet(const std::unordered_set<T>& set)
        : Column(_staticKind),
          _data(set)
    {
    }

    explicit ColumnSet(std::unordered_set<T>&& set) noexcept
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
        const ColumnSet<T>* otherCol = dynamic_cast<const ColumnSet<T>*>(other);
        msgbioassert(otherCol,
                     "ColumnSet::assign: other is not a ColumnSet of the same type");
        _data = otherCol->_data;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        msgbioassert(false, "ColumnSet::assignFromLine: not implemented for ColumnSet");
    }

    void clear() noexcept { _data.clear(); }

    void resize([[maybe_unused]] size_t size) override {
        throw FatalException("Attempted to resize ColumnSet.");
    }

    std::pair<Iterator, bool> insert(T&& val) { return _data.insert(val); }

    template <typename... Args>
    std::pair<Iterator, bool> emplace(Args&&... args) {
        return _data.emplace(std::forward<Args>(args)...);
    }

    // TODO: Iterator hinted insertions and emplacings

    Iterator erase(Iterator pos) { return _data.erase(pos); }
    ConstIterator erase(ConstIterator cpos) { return _data.erase(cpos); }
    size_t erase(const T& key) { return _data.erase(key); }

    void swap(ColumnSet& other) noexcept { _data.swap(other._data); }

    size_t count(const T& key) const { return _data.count(key); }
    // NOTE: C++20 templated version not added here

    Iterator find(const T& key) { return _data.find(key); }
    ConstIterator find(const T& key) const { return _data.find(key); }
    // NOTE: C++20 templated versions not added here

    bool contains(const T& key) const { return _data.contains(key); }
    // NOTE: C++20 templated version not added here

    void dump(std::ostream& out) const override {
        DebugDump::dumpString(out, "ColumnSet of size=" + std::to_string(size()));
        for (const auto& x : _data) {
            DebugDump::dump(out, x);
        }
    }

    std::unordered_set<T>& getRaw() { return _data; }
    const std::unordered_set<T>& getRaw() const { return _data; }

    static consteval auto staticKind() { return _staticKind; }

private:
    std::unordered_set<T> _data;

    static constexpr auto _staticKind = ColumnKind::getColumnKind<ColumnSet<T>>();
};
}
