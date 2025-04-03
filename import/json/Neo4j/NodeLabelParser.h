#pragma once

#include <nlohmann/json.hpp>

#include "Parser.h"
#include "writers/MetadataBuilder.h"

namespace db::json::neo4j {

using json = nlohmann::json;

class NodeLabelParser : public json::json_sax_t, public Parser {
public:
    explicit NodeLabelParser(MetadataBuilder* metadata)
        : _metadata(metadata)
    {
    }

    bool null() override {
        return true;
    }

    bool boolean(bool) override {
        return true;
    }

    bool number_integer(number_integer_t) override {
        return true;
    }

    bool number_unsigned(number_unsigned_t) override {
        return true;
    }

    bool number_float(number_float_t, const std::string&) override {
        return true;
    }

    bool string(string_t& val) override {
        if (_nesting == 6) {
            _metadata->getOrCreateLabel(val);
        }

        return true;
    }

    bool start_object(std::size_t) override {
        _nesting++;
        return true;
    }

    bool end_object() override {
        _nesting--;
        return true;
    }

    bool start_array(std::size_t) override {
        _nesting++;
        return true;
    }

    bool end_array() override {
        _nesting--;
        return true;
    }

    bool key(string_t&) override {
        return true;
    }

    bool binary(json::binary_t&) override {
        return true;
    }

    bool parse_error(std::size_t, const std::string&, const json::exception&) override {
        return false;
    }

private:
    size_t _nesting = 0;
    MetadataBuilder* _metadata {nullptr};
};

}
