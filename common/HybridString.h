#pragma once

#include <stddef.h>

#include <string>
#include <string_view>

class HybridString {
public:
    HybridString() = default;
    HybridString(std::string_view view);
    ~HybridString();

    HybridString(const HybridString&) = delete;
    HybridString(HybridString&&) = delete;
    HybridString& operator=(const HybridString&) = delete;
    HybridString& operator=(HybridString&&) = delete;

    std::string_view getView() const { return _owned ? std::string_view(_storage) : _view; }
    size_t size() const { return getView().size(); }
    bool empty() const { return getView().empty(); }
    const char* data() const { return getView().data(); }
    bool isOwned() const { return _owned; }

    void setView(std::string_view view);

    std::string& mutate();

private:
    std::string_view _view;
    std::string _storage;
    bool _owned {false};
};
