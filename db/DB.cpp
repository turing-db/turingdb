// Copyright 2023 Turing Biosystems Ltd.

#include "DB.h"

#include "ValueType.h"
#include "Network.h"
#include "NodeType.h"
#include "ComponentType.h"

using namespace db;

DB::DB()
{
    _int = new ValueType(ValueType::VK_INT);
    _unsigned = new ValueType(ValueType::VK_UNSIGNED);
    _bool = new ValueType(ValueType::VK_BOOL);
    _decimal = new ValueType(ValueType::VK_DECIMAL);
    _string = new ValueType(ValueType::VK_STRING);
}

DB::~DB() {
    delete _int;
    delete _unsigned;
    delete _bool;
    delete _decimal;
    delete _string;
}

DB* DB::create() {
    DB* db = new DB();
    return db;
}

StringRef DB::getString(const std::string& str) {
    return _strIndex.getString(str);
}

void DB::addNetwork(Network* net) {
    _networks[net->getName()] = net;
}

Network* DB::getNetwork(StringRef name) const {
    const auto foundIt = _networks.find(name);
    if (foundIt == _networks.end()) {
        return nullptr;
    }

    return foundIt->second;
}

NodeType* DB::getNodeType(StringRef name) const {
    const auto foundIt = _nodeTypes.find(name);
    if (foundIt == _nodeTypes.end()) {
        return nullptr;
    }

    return foundIt->second;
}

ComponentType* DB::getComponentType(StringRef name) const {
    const auto foundIt = _compTypes.find(name);
    if (foundIt == _compTypes.end()) {
        return nullptr;
    }

    return foundIt->second;
}

void DB::addNodeType(NodeType* nodeType) {
    _nodeTypes[nodeType->getName()] = nodeType;
}

void DB::addComponentType(ComponentType* compType) {
    _compTypes[compType->getName()] = compType;
}
