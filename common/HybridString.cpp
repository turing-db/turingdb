#include "HybridString.h"

HybridString::HybridString(std::string_view view)
    : _view(view)
{
}

HybridString::~HybridString() {
}

void HybridString::setView(std::string_view view) {
    _view = view;
    _storage.clear();
    _owned = false;
}

std::string& HybridString::mutate() {
    if (!_owned) {
        _storage.assign(_view);
        _view = std::string_view();
        _owned = true;
    }
    return _storage;
}
