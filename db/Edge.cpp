// Copyright 2023 Turing Biosystems Ltd.

#include "Edge.h"

using namespace db;

Edge::Edge(EdgeType* type, Node* source, Node* target)
    : _type(type),
    _source(source),
    _target(target)
{
}

Edge::~Edge() {
}
