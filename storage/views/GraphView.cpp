#include "GraphView.h"

#include "reader/GraphReader.h"

using namespace db;

GraphReader GraphView::read() const {
    return GraphReader(*this);
}

