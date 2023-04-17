// Copyright 2023 Turing Biosystems Ltd.

#include "NodeType.h"

#include "NodeDescriptor.h"

using namespace db;

NodeType::NodeType(StringRef name)
    : _name(name)
{
    _rootDesc = new NodeDescriptor(this);
}

NodeType::~NodeType() {
}
