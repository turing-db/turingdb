// Copyright 2023 Turing Biosystems Ltd.

#include "Edge.h"

#include "DB.h"
#include "Node.h"
#include "Network.h"
#include "EdgeType.h"

using namespace db;

Edge::Edge(EdgeType* type, Node* source, Node* target)
    : _type(type),
    _source(source),
    _target(target)
{
}

Edge::~Edge() {
}

Edge* Edge::create(DB* db, EdgeType* type, Node* source, Node* target) {
    Edge* edge = new Edge(type, source, target);
    db->addEdge(edge);

    source->addOutEdge(edge);
    target->addInEdge(edge);

    return edge;
}
