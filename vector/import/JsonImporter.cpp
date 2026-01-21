#include "JsonImporter.h"

#include <nlohmann/json.hpp>

#include "JsonSaxParser.h"

using namespace vec;

ImportResult<void> JsonImporter::import(BatchVectorCreate& batch,
                                        const std::string& data) {
    if (data.empty()) {
        return ImportError::result(ImportErrorCode::InvalidData, "JSON is empty");
    }

    JsonSaxParser parser(batch);
    nlohmann::json::sax_parse(data, &parser, nlohmann::json::input_format_t::json, true, true);

    return {};
}
