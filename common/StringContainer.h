#pragma once

#include <cstring>
#include <span>
#include <vector>
#include <string_view>

#include "BioAssert.h"

class StringContainer {
public:
    using ViewVector = std::vector<std::string_view>;

    StringContainer() = default;
    ~StringContainer() = default;
    StringContainer(StringContainer&& other) = default;
    StringContainer& operator=(StringContainer&& other) = default;
    StringContainer(const StringContainer& other) = delete;
    StringContainer& operator=(const StringContainer& other) = delete;

    void alloc(std::string_view content) {
        msgbioassert(!_built, "StringContainer was already built");

        const size_t offset = _chars.size();

        auto& repr = _reprs.emplace_back();
        repr._offset = offset;
        repr._size = content.size();

        _chars.resize(_chars.size() + content.size());
        std::memcpy(_chars.data() + offset, content.data(), content.size());
    }


    std::string_view getView(size_t index) const {
        msgbioassert(index < _views.size(), "String index invalid");
        return _views[index];
    }

    size_t size() const { return _views.size(); }

    void build() {
        msgbioassert(!_built, "StringContainer was already built");
        _views.resize(_reprs.size());

        for (size_t i = 0; i < _views.size(); i++) {
            const auto& repr = _reprs[i];
            _views[i] = {_chars.data() + repr._offset, repr._size};
        }

        _built = true;
        _reprs.clear();
    }

    void sort(const std::span<const size_t>& newOrder) {
        std::vector<char> newChars;
        ViewVector newViews;

        newChars.resize(_chars.size());
        newViews.resize(_views.size());

        size_t offset = 0;
        size_t i = 0;

        for (const size_t stringIndex : newOrder) {
            const std::string_view& v = _views[stringIndex];

            // Memcpy including null terminator
            char* dst = newChars.data() + offset;
            std::memcpy(dst, v.data(), v.size());
            newViews[i] = {dst, v.size()};
            offset += v.size();

            i++;
        }


        _views = std::move(newViews);
        _chars = std::move(newChars);
    }

    const ViewVector& get() const { return _views; }
    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

private:
    struct StringRepr {
        size_t _offset {};
        size_t _size {};
    };

    std::vector<char> _chars;
    ViewVector _views;

    // Only used before build()
    bool _built = false;
    std::vector<StringRepr> _reprs;
};

