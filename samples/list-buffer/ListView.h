#pragma once

#include <vector>

#include "ListBufferElementView.h"

namespace db {

class ListView {
public:
    void push_back(const ListBufferElementView& element);

    std::vector<ListBufferElementView>::iterator begin();
    std::vector<ListBufferElementView>::iterator end();

private:
    std::vector<ListBufferElementView> _elems;
};

}
