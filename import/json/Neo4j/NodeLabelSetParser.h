#pragma once

#include <nlohmann/json.hpp>

#include "writers/MetadataBuilder.h"
#include "Parser.h"
#include "labels/LabelSet.h"

namespace db::json::neo4j {

using json = nlohmann::json;

class NodeLabelSetParser: public json::json_sax_t, public Parser {
public:
    explicit NodeLabelSetParser(MetadataBuilder* metadata)
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
        if (_parsingLabelSets) {
            const LabelID labelId = _metadata->getOrCreateLabel(val);
            _labelSet.set(labelId);
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
        if (_nesting == 7) {
            _parsingLabelSets = true;
            _labelSet = LabelSet();
        }
        return true;
    }

    bool end_array() override {
        _nesting--;
        if(_parsingLabelSets){
            _metadata->getOrCreateLabelSet(_labelSet);
        }
        _parsingLabelSets = false;
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
    size_t _nesting {0};
    MetadataBuilder* _metadata {nullptr};
    LabelSet _labelSet;
    bool _parsingLabelSets {false};

};

}
