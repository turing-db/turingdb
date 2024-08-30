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
    enum class MatchType {
        EXACT,
        PREFIX,
        PREFIX_AND_LOC,
        SUBWORD
    };

    NodeSearch(db::DB* db);
    ~NodeSearch();

    void setDebug(bool debug) { _trace = debug; }

    void addProperty(const std::string& propName,
                     const std::string& value,
                     MatchType matchType = MatchType::SUBWORD);

    void addAllowedType(const db::NodeType* nodeType);
    void addId(db::DBIndex id);

    void run(std::vector<db::Node*>& result);

    static bool isApproximateMatch(const std::string& str,
                                   const std::string& expectedValue);
    static bool isPrefixMatch(const std::string& str,
                              const std::string& expectedValue);
    static bool isPrefixAndLocMatch(const std::string& str,
                                    const std::string& expectedValue);

private:
    using PropertyName = db::StringRef;
    using Values = std::vector<std::string>;

    db::DB* _db {nullptr};
    std::unordered_set<const db::NodeType*> _types;
    std::unordered_set<db::DBIndex> _ids;
    std::map<PropertyName, std::pair<Values, MatchType>> _properties;
    bool _trace {true};
};
