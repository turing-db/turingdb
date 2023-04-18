// Copyright 2023 Turing Biosystems Ltd.

#include "EdgeType.h"

using namespace db;

EdgeType::EdgeType(StringRef name,
                   ComponentType* sourceType,
                   ComponentType* targetType)
    : _name(name),
    _sourceType(sourceType),
    _targetType(targetType)
{
}

EdgeType::~EdgeType() {
}
