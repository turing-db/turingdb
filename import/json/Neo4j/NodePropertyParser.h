#pragma once

#include <nlohmann/json.hpp>

#include "writers/MetadataBuilder.h"

namespace db::json::neo4j {

using json = nlohmann::json;

class NodePropertyParser : public json::json_sax_t {
public:
    explicit NodePropertyParser(MetadataBuilder* metadata)
        : _metadata(metadata)
    {
    }

    bool null() override {
        if (_nesting == 6) {
            _keyOffset++;
        }
        return true;
    }

    bool boolean(bool) override {
        if (_nesting == 6) {
            _keyOffset++;
        }
        return true;
    }

    bool number_integer(number_integer_t) override {
        if (_nesting == 6) {
            _keyOffset++;
        }
        return true;
    }

    bool number_unsigned(number_unsigned_t) override {
        if (_nesting == 6) {
            _keyOffset++;
        }
        return true;
    }

    bool number_float(number_float_t, const string_t&) override {
        if (_nesting == 6) {
            _keyOffset++;
        }

        return true;
    }

    bool string(string_t& val) override {
        if (_nesting == 6) {
            _keyOffset++;

            if (_keyOffset == 3) {
                _currentPropName = std::move(val);
                return true;
            }
        }

        if (_nesting == 7) {
            if (val == "Integer" || val == "Long") {
                _metadata->getOrCreatePropertyType(_currentPropName + " (Int64)", ValueType::Int64);
                _metadata->getOrCreatePropertyType(_currentPropName + " (UInt64)", ValueType::UInt64);
                return true;
            }

            if (val == "Double") {
                _metadata->getOrCreatePropertyType(_currentPropName + " (Double)", ValueType::Double);
                return true;
            }

            if (val == "Boolean") {
                _metadata->getOrCreatePropertyType(_currentPropName + " (Bool)", ValueType::Bool);
                return true;
            }

            if (val == "String" || val == "Date") {
                _metadata->getOrCreatePropertyType(_currentPropName + " (String)", ValueType::String);
                return true;
            }

            return true;
        }
        return true;
    }

    bool start_object(std::size_t) override {
        if (_nesting == 6) {
            _keyOffset++;
        }

        _nesting++;
        return true;
    }

    bool end_object() override {
        _nesting--;
        return true;
    }

    bool start_array(std::size_t) override {
        if (_nesting == 6) {
            _keyOffset++;
        }

        _nesting++;

        return true;
    }

    bool end_array() override {
        _nesting--;
        return true;
    }

    bool key(string_t& k) override {
        if (k == "row") {
            // Entered a new 'row' array, of type:
            //  we need propName which is the second string in the array ↓
            // ["nodetype", [label1, label2, ...], "propName", ["ValueType"], true]
            _keyOffset = 0;
        }
        return true;
    }

    bool binary(json::binary_t&) override {
        if (_nesting == 6) {
            _keyOffset++;
        }
        return true;
    }

    bool parse_error(std::size_t, const std::string&, const json::exception&) override {
        return false;
    }

private:
    MetadataBuilder* _metadata {nullptr};
    std::string _currentPropName;
    size_t _nesting {0};
    size_t _keyOffset {0};
};

}
