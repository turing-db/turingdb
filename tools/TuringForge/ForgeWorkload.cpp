#include "ForgeWorkload.h"

#include <fstream>

#include <nlohmann/json.hpp>

#include "TuringException.h"

using namespace forge;

ForgeWorkload::ForgeWorkload(const std::string& path)
        : _path(path)
{
}

void ForgeWorkload::parseFile() {
    std::ifstream file(_path);
    if (!file.is_open()) {
        throw TuringException("Failed to open queries file: " + _path);
    }

    // ordered_json preserves the file's key order, so test loads run in the
    // order they are written rather than alphabetically.
    nlohmann::ordered_json document;
    try {
        document = nlohmann::ordered_json::parse(file);
    } catch (const nlohmann::json::exception& e) {
        throw TuringException("Failed to parse queries JSON '" + _path + "': " + e.what());
    }

    const auto queriesField = document.find("queries");
    if (queriesField == document.end() || !queriesField->is_object()) {
        throw TuringException("Queries JSON must have a top-level \"queries\" object");
    }

    for (const auto& entry : queriesField->items()) {
        const std::string& label = entry.key();
        const auto& value = entry.value();

        // A load is either a bare array of queries (a read load), or an object of
        // the form { "queries": [...], "write": true } marking it a write load.
        const nlohmann::ordered_json* queryArray = nullptr;
        bool isWrite = false;
        if (value.is_array()) {
            queryArray = &value;
        } else if (value.is_object()) {
            const auto queriesEntry = value.find("queries");
            if (queriesEntry == value.end() || !queriesEntry->is_array()) {
                throw TuringException("Test load \"" + label + "\" object must have a \"queries\" array");
            }
            queryArray = &(*queriesEntry);

            const auto writeEntry = value.find("write");
            if (writeEntry != value.end()) {
                if (!writeEntry->is_boolean()) {
                    throw TuringException("Test load \"" + label + "\" field \"write\" must be a boolean");
                }
                isWrite = writeEntry->get<bool>();
            }
        } else {
            throw TuringException("Test load \"" + label + "\" must be an array of queries or an object with a \"queries\" array");
        }

        if (queryArray->empty()) {
            throw TuringException("Test load \"" + label + "\" must have a non-empty \"queries\" array");
        }

        _labelToIndex[label] = _labels.size();
        _labels.push_back(label);
        _writeLoads.push_back(isWrite);

        _queries.emplace_back();
        std::vector<std::string>& loadQueries = _queries.back();
        for (const auto& query : *queryArray) {
            if (!query.is_string()) {
                throw TuringException("Test load \"" + label + "\" contains a non-string query");
            }
            loadQueries.push_back(query.get<std::string>());
        }
    }

    if (_labels.empty()) {
        throw TuringException("Queries JSON \"queries\" object is empty");
    }
}

bool ForgeWorkload::findLoad(const std::string& label, size_t& outIndex) const {
    const auto found = _labelToIndex.find(label);
    if (found == _labelToIndex.end()) {
        return false;
    }

    outIndex = found->second;
    return true;
}
