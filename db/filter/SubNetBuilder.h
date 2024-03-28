#pragma once

#include <unordered_map>

#include "Writeback.h"

namespace db {
class DB;
class Network;
class DBEntity;
}

class SubNetBuilder {
public:
    SubNetBuilder(db::DB* db, db::Network* subNet);
    ~SubNetBuilder();

    void addNode(db::Node* node);

    db::Node* getNodeInSubNet(const db::Node* node) const;

private:
    db::Network* _subNet {nullptr};
    db::Writeback _wb;
    std::unordered_map<const db::Node*, db::Node*> _subNetNodeMap;

    void cloneProperties(const db::DBEntity* src, db::DBEntity* dest);
};
