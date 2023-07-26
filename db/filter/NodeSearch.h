#pragma once

#include <string>
#include <vector>
#include <unordered_set>
#include <utility>
#include <map>

#include "DBIndex.h"
#include "StringRef.h"

namespace db {
class DB;
class Node;
class NodeType;
}

class NodeSearch {
public:
    NodeSearch(db::DB* db);
    ~NodeSearch();

    void addProperty(const std::string& propName,
                     const std::string& value,
                     bool exact=false);

    void addAllowedType(const db::NodeType* nodeType);
    void addId(db::DBIndex id);

    void run(std::vector<db::Node*>& result);

private:
    using PropertyName = db::StringRef;
    using Values = std::vector<std::string>;

    db::DB* _db {nullptr};
    std::unordered_set<const db::NodeType*> _types;
    std::unordered_set<db::DBIndex> _ids;
    std::map<PropertyName, std::pair<Values, bool>> _properties;
};
