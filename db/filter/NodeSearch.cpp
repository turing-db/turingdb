#include "NodeSearch.h"

#include <sstream>

#include "DB.h"
#include "Node.h"
#include "PropertyType.h"

using namespace db;

NodeSearch::NodeSearch(db::DB* db)
    : _db(db)
{
}

NodeSearch::~NodeSearch() {
}

void NodeSearch::addProperty(const std::string& propName,
                             const std::string& value,
                             MatchType matchType) {
    auto& propEntry = _properties[_db->getString(propName)];
    propEntry.first.push_back(value);
    propEntry.second = matchType;
}

void NodeSearch::addAllowedType(const db::NodeType* nodeType) {
    _types.insert(nodeType);
}

void NodeSearch::addId(db::DBIndex id) {
    _ids.insert(id);
}

void NodeSearch::run(std::vector<db::Node*>& result) {
    const bool checkType = !_types.empty();
    const bool checkProperties = !_properties.empty();
    const bool checkIds = !_ids.empty();

    for (const auto& [id, node] : _db->nodes()) {
        if (checkType) {
            const auto typeIt = _types.find(node->getType());
            if (typeIt == _types.end()) {
                continue;
            }
        }

        if (checkIds) {
            const auto idIt = _ids.find(node->getIndex());
            if (idIt == _ids.end()) {
                continue;
            }
        }

        if (checkProperties) {
            bool allPropsFound = true;
            for (const auto& [expectedPropName, expectedEntry] : _properties) {
                const auto& expectedValueSet = expectedEntry.first;
                const MatchType matchType = expectedEntry.second;
                bool propFound = false;
                for (const auto& [propType, value] : node->properties()) {
                    if (!propType->getValueType().isString()) {
                        continue;
                    }
                    if (propType->getName() != expectedPropName) {
                        continue;
                    }

                    const std::string& valueStr = value.getString();
                    for (const auto& expectedValue : expectedValueSet) {
                        if (matchType == MatchType::EXACT) {
                            if (valueStr == expectedValue) {
                                propFound = true;
                                break;
                            }
                        } else if (matchType == MatchType::PREFIX) {
                            if (isPrefixMatch(valueStr, expectedValue)) {
                                propFound = true;
                                break;
                            }
                        } else if (matchType == MatchType::PREFIX_AND_LOC) {
                            if (isPrefixAndLocMatch(valueStr, expectedValue)) {
                                propFound = true;
                                break;
                            }
                        } else {
                            if (isApproximateMatch(valueStr, expectedValue)) {
                                propFound = true;
                                break;
                            }
                        }
                    }

                    if (propFound) {
                        break;
                    }
                }

                if (!propFound) {
                    allPropsFound = false;
                }
            }

            if (!allPropsFound) {
                continue;
            }
        }

        result.push_back(node);
    }
}

bool NodeSearch::isApproximateMatch(const std::string& str, const std::string& expectedValue) {
    if (expectedValue.size() >= str.size()) {
        return str == expectedValue;
    }

    // If the expectedValue is shorter than str we look if expectedValue
    // is a word of str
    std::stringstream ss(str);
    std::string word;
    while (ss >> word) {
        if (word == expectedValue) {
            return true;
        }
    }
    return false;
}

bool NodeSearch::isPrefixMatch(const std::string& str, const std::string& expectedValue) {
    if (expectedValue.size() >= str.size()) {
        return str == expectedValue;
    }

    std::stringstream ss(str);
    std::string word;
    ss >> word;
    return word == expectedValue;
}

bool NodeSearch::isPrefixAndLocMatch(const std::string& str, const std::string& expectedValue) {
    if (expectedValue.size() >= str.size()) {
        return str == expectedValue;
    }

    std::stringstream ss(str);
    std::string word;

    // First word must match expectedValue
    ss >> word;
    if (word != expectedValue) {
        return false;
    }

    // Second word must begin with [
    ss >> word;
    return word.starts_with("[");
}
