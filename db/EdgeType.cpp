// Copyright 2023 Turing Biosystems Ltd.

#include "EdgeType.h"

#include "DB.h"
#include "NodeType.h"

using namespace db;

EdgeType::EdgeType(StringRef name,
                   NodeType* sourceType,
                   NodeType* targetType)
    : _name(name),
    _sourceType(sourceType),
    _targetType(targetType)
{
}

EdgeType::~EdgeType() {
}

EdgeType* EdgeType::create(DB* db,
                           StringRef name,
                           NodeType* source,
                           NodeType* target) {
    EdgeType* edgeType = new EdgeType(name, source, target);
    db->addEdgeType(edgeType);

    source->addOutEdgeType(edgeType);
    target->addInEdgeType(edgeType);

    return edgeType;
}
