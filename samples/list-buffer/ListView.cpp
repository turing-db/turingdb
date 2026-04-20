#include "ListView.h"

using namespace db;

void ListView::push_back(const ListBufferElementView& element) {
    _elems.push_back(element);
}

std::vector<ListBufferElementView>::iterator ListView::begin() {
    return std::begin(_elems);
}

std::vector<ListBufferElementView>::iterator ListView::end() {
    return std::end(_elems);
}
