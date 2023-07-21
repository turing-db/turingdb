#include "NodeSearch.h"

#include <sstream>

#include "DB.h"
#include "Node.h"
#include "PropertyType.h"

#include "BioLog.h"

using namespace db;
using namespace Log;

namespace {

bool isApproximateMatch(const std::string& str, const std::string& expectedValue) {
    if (expectedValue.size() >= str.size()) {
        return str == expectedValue;
    }

    // If the expectedValue is shorter than str we compare word by word
    std::stringstream ss(str);
    std::string word;
    while (ss >> word) {
        if (word == expectedValue) {
            return true;
        }
    }
    return false;
}

}

NodeSearch::NodeSearch(db::DB* db)
    : _db(db)
{
}

NodeSearch::~NodeSearch() {
}

void NodeSearch::addProperty(const std::string& propName,
                             const std::string& value,
                             bool exact) {
    auto& propEntry = _properties[_db->getString(propName)];
    propEntry.first.push_back(value);
    propEntry.second &= exact;
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
                const bool exact = expectedEntry.second;
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
                        if (exact) {
                            if (valueStr == expectedValue) {
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
